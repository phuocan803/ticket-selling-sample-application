const crypto = require('crypto');
const { promisify } = require('util');

const cookieSession = require('cookie-session');
const express = require('express');
const jwt = require('jsonwebtoken');
const { body, validationResult } = require('express-validator');
const client = require('prom-client');
const { createClient } = require('redis');
const { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');

const app = express();
const port = process.env.PORT || 3000;
const jwtKey = process.env.JWT_KEY || 'dev-jwt-key';
const scryptAsync = promisify(crypto.scrypt);
const sqsRegion = process.env.AWS_REGION || '';
const sqsQueueUrl = process.env.SQS_AUTH_QUEUE_URL || '';
const enableWorker = process.env.ENABLE_AUTH_WORKER === 'true';
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);
const sqsClient = sqsRegion && sqsQueueUrl ? new SQSClient({ region: sqsRegion }) : null;
const authJobs = [];
const register = new client.Registry();

client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the auth service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

// In-memory store for sprint bootstrap. Will be replaced by persistent DB in next phase.
const usersByEmail = new Map();
const redisUrl = process.env.REDIS_URL || '';
const redisPrefix = process.env.REDIS_PREFIX || 'auth';
const redisClient = redisUrl ? createClient({ url: redisUrl }) : null;
let redisConnectPromise = null;

if (redisClient) {
  redisClient.on('error', (err) => {
    console.error('auth redis error', err);
  });
}

const ensureRedisConnection = async () => {
  if (!redisClient) {
    return false;
  }

  if (redisClient.isOpen) {
    return true;
  }

  if (!redisConnectPromise) {
    redisConnectPromise = redisClient.connect().catch((err) => {
      redisConnectPromise = null;
      throw err;
    });
  }

  await redisConnectPromise;
  return true;
};

const userKey = (email) => `${redisPrefix}:users:${String(email).toLowerCase()}`;

const getUserByEmail = async (email) => {
  const normalizedEmail = String(email).toLowerCase();

  if (!redisClient) {
    return usersByEmail.get(normalizedEmail) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.get(userKey(normalizedEmail));
  return raw ? JSON.parse(raw) : null;
};

const saveUser = async (user) => {
  const normalizedEmail = String(user.email).toLowerCase();
  const next = { ...user, email: normalizedEmail };

  if (!redisClient) {
    usersByEmail.set(normalizedEmail, next);
    return next;
  }

  await ensureRedisConnection();
  await redisClient.set(userKey(normalizedEmail), JSON.stringify(next));
  return next;
};

app.set('trust proxy', true);
app.use(express.json());
app.use((req, res, next) => {
  const route = req.path;
  res.on('finish', () => {
    httpRequestsTotal.inc({
      method: req.method,
      route,
      status_code: String(res.statusCode)
    });
  });
  next();
});
app.use(
  cookieSession({
    signed: false,
    secure: false
  })
);

const toSafeUser = (user) => ({ id: user.id, email: user.email });

const toHash = async (password) => {
  const salt = crypto.randomBytes(8).toString('hex');
  const buf = await scryptAsync(password, salt, 64);
  return `${buf.toString('hex')}.${salt}`;
};

const compareHash = async (storedPassword, suppliedPassword) => {
  const [hashedPassword, salt] = storedPassword.split('.');
  const buf = await scryptAsync(suppliedPassword, salt, 64);
  return buf.toString('hex') === hashedPassword;
};

const requireValidRequest = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  return next();
};

const decodeCurrentUser = (req) => {
  const token = req.session && req.session.jwt;
  if (!token) {
    return null;
  }
  try {
    return jwt.verify(token, jwtKey);
  } catch (_err) {
    return null;
  }
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const publishAuthEvent = async (eventType, user) => {
  if (!sqsClient || !sqsQueueUrl) {
    return;
  }

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsQueueUrl,
      MessageBody: JSON.stringify({
        type: eventType,
        data: {
          userId: user.id,
          email: user.email
        },
        emittedAt: new Date().toISOString()
      })
    })
  );
};

const startAuthWorker = async () => {
  if (!enableWorker || !sqsClient || !sqsQueueUrl) {
    return;
  }

  console.log('auth SQS worker enabled');

  while (true) {
    try {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: sqsQueueUrl,
          MaxNumberOfMessages: sqsMaxMessages,
          WaitTimeSeconds: sqsWaitTimeSeconds,
          VisibilityTimeout: sqsVisibilityTimeout
        })
      );

      const messages = response.Messages || [];
      for (const message of messages) {
        if (!message.ReceiptHandle || !message.Body) {
          continue;
        }

        try {
          authJobs.unshift({
            id: message.MessageId,
            body: message.Body,
            processedAt: new Date().toISOString()
          });
          authJobs.splice(20);

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsQueueUrl,
              ReceiptHandle: message.ReceiptHandle
            })
          );
        } catch (err) {
          console.error('auth worker failed to process message', err);
        }
      }
    } catch (err) {
      console.error('auth worker polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'auth', status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.post(
  '/api/users/signup',
  [
    body('email').isEmail().withMessage('Email must be valid'),
    body('password')
      .trim()
      .isLength({ min: 4, max: 20 })
      .withMessage('Password must be between 4 and 20 characters')
  ],
  requireValidRequest,
  async (req, res) => {
    const email = String(req.body.email || '').toLowerCase();
    const { password } = req.body;

    const existingUser = await getUserByEmail(email);
    if (existingUser) {
      return res.status(400).json({ message: 'Email already in use' });
    }

    const user = await saveUser({
      id: crypto.randomUUID(),
      email,
      password: await toHash(password)
    });

    try {
      await publishAuthEvent('user.signup', user);
    } catch (err) {
      console.error('failed to publish user.signup event', err);
    }

    const userJwt = jwt.sign({ id: user.id, email: user.email }, jwtKey);
    req.session = { jwt: userJwt };

    return res.status(201).json(toSafeUser(user));
  }
);

app.post(
  '/api/users/signin',
  [
    body('email').isEmail().withMessage('Email must be valid'),
    body('password').trim().notEmpty().withMessage('Password is required')
  ],
  requireValidRequest,
  async (req, res) => {
    const email = String(req.body.email || '').toLowerCase();
    const { password } = req.body;
    const existingUser = usersByEmail.get(email);
    const persistedUser = await getUserByEmail(email);
    const user = persistedUser || existingUser;
    if (!user) {
      return res.status(400).json({ message: 'Invalid credentials' });
    }

    const isValidPassword = await compareHash(user.password, password);
    if (!isValidPassword) {
      return res.status(400).json({ message: 'Invalid credentials' });
    }

    const userJwt = jwt.sign({ id: user.id, email: user.email }, jwtKey);
    req.session = { jwt: userJwt };

    try {
      await publishAuthEvent('user.signin', user);
    } catch (err) {
      console.error('failed to publish user.signin event', err);
    }

    return res.status(200).json(toSafeUser(user));
  }
);

app.post('/api/users/signout', (req, res) => {
  req.session = null;
  res.status(200).json({});
});

app.get('/api/users/currentuser', (req, res) => {
  const currentUser = decodeCurrentUser(req);
  res.status(200).json({ currentUser });
});

app.get('/', (_req, res) => {
  res.status(200).json({
    service: 'auth',
    message: 'ticket-selling auth service is running',
    integrations: {
      sqsEventPublishEnabled: Boolean(sqsClient && sqsQueueUrl),
      sqsWorkerEnabled: Boolean(enableWorker && sqsClient && sqsQueueUrl),
      redisPersistenceEnabled: Boolean(redisClient),
      recentJobs: authJobs.length
    }
  });
});

app.get('/api/auth/jobs', (_req, res) => {
  res.status(200).json(authJobs);
});

app.listen(port, () => {
  console.log('auth service listening on port ' + port);
  startAuthWorker().catch((err) => {
    console.error('auth worker failed to start', err);
  });
});
// API improvements and optimizations
// API security improvements per code review
// Enhanced auth validation and security
