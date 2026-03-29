const crypto = require('crypto');

const express = require('express');
const { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');
const client = require('prom-client');
const { createClient } = require('redis');

const app = express();
const port = process.env.PORT || 3000;

const sqsRegion = process.env.AWS_REGION || '';
const sqsExpirationQueueUrl = process.env.SQS_EXPIRATION_QUEUE_URL || '';
const sqsExpirationEventsQueueUrl = process.env.SQS_EXPIRATION_EVENTS_QUEUE_URL || '';
const enableWorker = process.env.ENABLE_EXPIRATION_WORKER === 'true';
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);

const sqsClient = sqsRegion ? new SQSClient({ region: sqsRegion }) : null;
const expirations = new Map();
const register = new client.Registry();
const redisUrl = process.env.REDIS_URL || '';
const redisPrefix = process.env.REDIS_PREFIX || 'expiration';
const redisClient = redisUrl ? createClient({ url: redisUrl }) : null;
let redisConnectPromise = null;

client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the expiration service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

if (redisClient) {
  redisClient.on('error', (err) => {
    console.error('expiration redis error', err);
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

const expirationsHashKey = `${redisPrefix}:expirations`;

const saveExpiration = async (expiration) => {
  expirations.set(expiration.orderId, expiration);

  if (!redisClient) {
    return;
  }

  await ensureRedisConnection();
  await redisClient.hSet(expirationsHashKey, expiration.orderId, JSON.stringify(expiration));
};

const getExpirationByOrderId = async (orderId) => {
  if (!redisClient) {
    return expirations.get(orderId) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.hGet(expirationsHashKey, orderId);
  return raw ? JSON.parse(raw) : null;
};

const listExpirations = async () => {
  if (!redisClient) {
    return Array.from(expirations.values());
  }

  await ensureRedisConnection();
  const values = await redisClient.hVals(expirationsHashKey);
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const parseOrderEvent = (messageBody) => {
  const raw = JSON.parse(messageBody);
  const data = raw && raw.data ? raw.data : raw;
  const orderId = data && (data.orderId || data.id);
  if (!orderId) {
    return null;
  }
  return {
    orderId,
    ticketId: data.ticketId,
    amount: Number(data.amount || 0),
    quantity: Number(data.quantity || 1)
  };
};

const publishExpirationEvent = async (expiration) => {
  if (!sqsClient || !sqsExpirationEventsQueueUrl) {
    return;
  }

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsExpirationEventsQueueUrl,
      MessageBody: JSON.stringify({
        type: 'order.expired',
        data: expiration,
        emittedAt: new Date().toISOString()
      })
    })
  );
};

const createExpiration = async (orderEvent) => {
  const existingExpiration = await getExpirationByOrderId(orderEvent.orderId);
  if (existingExpiration) {
    return existingExpiration;
  }

  const expiration = {
    id: crypto.randomUUID(),
    orderId: orderEvent.orderId,
    ticketId: orderEvent.ticketId || null,
    amount: Number(orderEvent.amount || 0),
    quantity: Number(orderEvent.quantity || 1),
    status: 'expired',
    expiredAt: new Date().toISOString()
  };

  await saveExpiration(expiration);
  await publishExpirationEvent(expiration);
  return expiration;
};

const startExpirationWorker = async () => {
  if (!enableWorker || !sqsClient || !sqsExpirationQueueUrl) {
    return;
  }

  console.log('expiration SQS worker enabled');

  while (true) {
    try {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: sqsExpirationQueueUrl,
          MaxNumberOfMessages: sqsMaxMessages,
          WaitTimeSeconds: sqsWaitTimeSeconds,
          VisibilityTimeout: sqsVisibilityTimeout
        })
      );

      const messages = response.Messages || [];
      for (const message of messages) {
        const receiptHandle = message.ReceiptHandle;
        if (!receiptHandle || !message.Body) {
          continue;
        }

        try {
          const orderEvent = parseOrderEvent(message.Body);
          if (orderEvent) {
            await createExpiration(orderEvent);
          }

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsExpirationQueueUrl,
              ReceiptHandle: receiptHandle
            })
          );
        } catch (err) {
          console.error('failed to process expiration message', err);
        }
      }
    } catch (err) {
      console.error('expiration worker polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'expiration', status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.get('/api/expirations', async (_req, res) => {
  res.status(200).json(await listExpirations());
});

app.get('/api/expirations/:orderId', async (req, res) => {
  const item = await getExpirationByOrderId(req.params.orderId);
  if (!item) {
    return res.status(404).json({ message: 'expiration not found' });
  }
  return res.status(200).json(item);
});

app.get('/', (_req, res) => {
  res.status(200).json({
    service: 'expiration',
    message: 'ticket-selling expiration service is running',
    integrations: {
      sqsWorkerEnabled: Boolean(enableWorker && sqsClient && sqsExpirationQueueUrl),
      sqsExpirationPublishEnabled: Boolean(sqsClient && sqsExpirationEventsQueueUrl),
      redisPersistenceEnabled: Boolean(redisClient)
    }
  });
});

app.listen(port, () => {
  console.log('expiration service listening on port ' + port);
  startExpirationWorker().catch((err) => {
    console.error('expiration worker failed to start', err);
  });
});
