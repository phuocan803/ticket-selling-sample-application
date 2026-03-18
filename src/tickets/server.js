const crypto = require('crypto');

const express = require('express');
const { body, param, validationResult } = require('express-validator');
const client = require('prom-client');
const { createClient } = require('redis');
const { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');

const app = express();
const port = process.env.PORT || 3000;
const sqsRegion = process.env.AWS_REGION || '';
const sqsQueueUrl = process.env.SQS_TICKETS_QUEUE_URL || '';
const enableWorker = process.env.ENABLE_TICKETS_WORKER === 'true';
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);
const sqsClient = sqsRegion && sqsQueueUrl ? new SQSClient({ region: sqsRegion }) : null;
const ticketsJobs = [];
const register = new client.Registry();
const tickets = new Map();
const redisUrl = process.env.REDIS_URL || '';
const redisPrefix = process.env.REDIS_PREFIX || 'tickets';
const redisClient = redisUrl ? createClient({ url: redisUrl }) : null;
let redisConnectPromise = null;

client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the tickets service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

if (redisClient) {
  redisClient.on('error', (err) => {
    console.error('tickets redis error', err);
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

const ticketsHashKey = `${redisPrefix}:tickets`;

const saveTicket = async (ticket) => {
  tickets.set(ticket.id, ticket);

  if (!redisClient) {
    return;
  }

  await ensureRedisConnection();
  await redisClient.hSet(ticketsHashKey, ticket.id, JSON.stringify(ticket));
};

const getTicketById = async (id) => {
  if (!redisClient) {
    return tickets.get(id) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.hGet(ticketsHashKey, id);
  return raw ? JSON.parse(raw) : null;
};

const listTickets = async () => {
  if (!redisClient) {
    return Array.from(tickets.values());
  }

  await ensureRedisConnection();
  const values = await redisClient.hVals(ticketsHashKey);
  return values.map((value) => JSON.parse(value));
};

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

const requireValidRequest = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  return next();
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const publishTicketEvent = async (eventType, ticket) => {
  if (!sqsClient || !sqsQueueUrl) {
    return;
  }

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsQueueUrl,
      MessageBody: JSON.stringify({
        type: eventType,
        data: ticket,
        emittedAt: new Date().toISOString()
      })
    })
  );
};

const startTicketsWorker = async () => {
  if (!enableWorker || !sqsClient || !sqsQueueUrl) {
    return;
  }

  console.log('tickets SQS worker enabled');

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
          ticketsJobs.unshift({
            id: message.MessageId,
            body: message.Body,
            processedAt: new Date().toISOString()
          });
          ticketsJobs.splice(20);

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsQueueUrl,
              ReceiptHandle: message.ReceiptHandle
            })
          );
        } catch (err) {
          console.error('tickets worker failed to process message', err);
        }
      }
    } catch (err) {
      console.error('tickets worker polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'tickets', status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.post(
  '/api/tickets',
  [
    body('title').trim().notEmpty().withMessage('title is required'),
    body('price').isFloat({ min: 1 }).withMessage('price must be greater than 0')
  ],
  requireValidRequest,
  async (req, res) => {
    const ticket = {
      id: crypto.randomUUID(),
      title: req.body.title,
      price: Number(req.body.price),
      status: 'available',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await saveTicket(ticket);

    try {
      await publishTicketEvent('ticket.created', ticket);
    } catch (err) {
      console.error('failed to publish ticket.created event', err);
    }

    return res.status(201).json(ticket);
  }
);

app.get('/api/tickets', async (_req, res) => {
  return res.status(200).json(await listTickets());
});

app.get(
  '/api/tickets/:id',
  [param('id').isUUID().withMessage('id must be a valid UUID')],
  requireValidRequest,
  async (req, res) => {
    const ticket = await getTicketById(req.params.id);
    if (!ticket) {
      return res.status(404).json({ message: 'ticket not found' });
    }
    return res.status(200).json(ticket);
  }
);

app.put(
  '/api/tickets/:id',
  [
    param('id').isUUID().withMessage('id must be a valid UUID'),
    body('title').optional().trim().notEmpty().withMessage('title cannot be empty'),
    body('price').optional().isFloat({ min: 1 }).withMessage('price must be greater than 0'),
    body('status')
      .optional()
      .isIn(['available', 'reserved', 'sold'])
      .withMessage('status must be available|reserved|sold')
  ],
  requireValidRequest,
  async (req, res) => {
    const ticket = await getTicketById(req.params.id);
    if (!ticket) {
      return res.status(404).json({ message: 'ticket not found' });
    }

    const next = {
      ...ticket,
      ...req.body,
      price: req.body.price !== undefined ? Number(req.body.price) : ticket.price,
      updatedAt: new Date().toISOString()
    };

    await saveTicket(next);

    try {
      await publishTicketEvent('ticket.updated', next);
    } catch (err) {
      console.error('failed to publish ticket.updated event', err);
    }

    return res.status(200).json(next);
  }
);

app.get('/', (_req, res) => {
  res.status(200).json({
    service: 'tickets',
    message: 'ticket-selling tickets service is running',
    integrations: {
      sqsEventPublishEnabled: Boolean(sqsClient && sqsQueueUrl),
      sqsWorkerEnabled: Boolean(enableWorker && sqsClient && sqsQueueUrl),
      redisPersistenceEnabled: Boolean(redisClient),
      recentJobs: ticketsJobs.length
    }
  });
});

app.get('/api/tickets/jobs', (_req, res) => {
  res.status(200).json(ticketsJobs);
});

app.listen(port, () => {
  console.log('tickets service listening on port ' + port);
  startTicketsWorker().catch((err) => {
    console.error('tickets worker failed to start', err);
  });
});
