const crypto = require('crypto');

const express = require('express');
const { body, param, validationResult } = require('express-validator');
const { SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');
const { DeleteMessageCommand, ReceiveMessageCommand } = require('@aws-sdk/client-sqs');
const client = require('prom-client');
const { createClient } = require('redis');

const app = express();
const port = process.env.PORT || 3000;

const sqsQueueUrl = process.env.SQS_ORDER_EVENTS_QUEUE_URL || '';
const sqsOrdersServiceQueueUrl = process.env.SQS_ORDERS_SERVICE_QUEUE_URL || '';
const sqsExpirationQueueUrl = process.env.SQS_EXPIRATION_QUEUE_URL || '';
const sqsRegion = process.env.AWS_REGION || '';
const enableOrdersWorker = process.env.ENABLE_ORDERS_WORKER === 'true';
const expirationDelaySeconds = Number(process.env.EXPIRATION_DELAY_SECONDS || 60);
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);
const sqsClient = sqsRegion ? new SQSClient({ region: sqsRegion }) : null;
const orderJobs = [];
const register = new client.Registry();
const orders = new Map();
const redisUrl = process.env.REDIS_URL || '';
const redisPrefix = process.env.REDIS_PREFIX || 'orders';
const redisClient = redisUrl ? createClient({ url: redisUrl }) : null;
let redisConnectPromise = null;

client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the orders service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

if (redisClient) {
  redisClient.on('error', (err) => {
    console.error('orders redis error', err);
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

const ordersHashKey = `${redisPrefix}:orders`;

const saveOrder = async (order) => {
  orders.set(order.id, order);

  if (!redisClient) {
    return;
  }

  await ensureRedisConnection();
  await redisClient.hSet(ordersHashKey, order.id, JSON.stringify(order));
};

const getOrderById = async (id) => {
  if (!redisClient) {
    return orders.get(id) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.hGet(ordersHashKey, id);
  return raw ? JSON.parse(raw) : null;
};

const listOrders = async () => {
  if (!redisClient) {
    return Array.from(orders.values());
  }

  await ensureRedisConnection();
  const values = await redisClient.hVals(ordersHashKey);
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

const startOrdersWorker = async () => {
  if (!enableOrdersWorker || !sqsClient || !sqsOrdersServiceQueueUrl) {
    return;
  }

  console.log('orders SQS worker enabled');

  while (true) {
    try {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: sqsOrdersServiceQueueUrl,
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
          orderJobs.unshift({
            id: message.MessageId,
            body: message.Body,
            processedAt: new Date().toISOString()
          });
          orderJobs.splice(20);

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsOrdersServiceQueueUrl,
              ReceiptHandle: message.ReceiptHandle
            })
          );
        } catch (err) {
          console.error('orders worker failed to process message', err);
        }
      }
    } catch (err) {
      console.error('orders worker polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

const publishOrdersServiceEvent = async (order) => {
  if (!sqsClient || !sqsOrdersServiceQueueUrl) {
    return;
  }

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsOrdersServiceQueueUrl,
      MessageBody: JSON.stringify({
        type: 'order.accepted',
        data: {
          orderId: order.id,
          ticketId: order.ticketId,
          quantity: order.quantity,
          status: order.status,
          amount: order.amount,
          createdAt: order.createdAt
        },
        emittedAt: new Date().toISOString()
      })
    })
  );
};

const publishOrderCreated = async (order) => {
  if (!sqsClient || !sqsQueueUrl) {
    return;
  }

  const payload = {
    type: 'order.created',
    data: {
      orderId: order.id,
      ticketId: order.ticketId,
      quantity: order.quantity,
      status: order.status,
      amount: order.amount,
      createdAt: order.createdAt
    },
    emittedAt: new Date().toISOString()
  };

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsQueueUrl,
      MessageBody: JSON.stringify(payload)
    })
  );

  if (sqsExpirationQueueUrl) {
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: sqsExpirationQueueUrl,
        DelaySeconds: expirationDelaySeconds,
        MessageBody: JSON.stringify(payload)
      })
    );
  }
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'orders', status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.post(
  '/api/orders',
  [
    body('ticketId').trim().notEmpty().withMessage('ticketId is required'),
    body('quantity')
      .optional()
      .isInt({ min: 1, max: 10 })
      .withMessage('quantity must be between 1 and 10')
  ],
  requireValidRequest,
  async (req, res) => {
    const quantity = Number(req.body.quantity || 1);
    const order = {
      id: crypto.randomUUID(),
      ticketId: req.body.ticketId,
      quantity,
      status: 'created',
      amount: quantity * 10,
      createdAt: new Date().toISOString()
    };

    await saveOrder(order);

    try {
      await publishOrdersServiceEvent(order);
      await publishOrderCreated(order);
    } catch (err) {
      console.error('failed to publish order async events', err);
    }

    return res.status(201).json(order);
  }
);

app.get(
  '/api/orders/:id',
  [param('id').isUUID().withMessage('id must be a valid UUID')],
  requireValidRequest,
  async (req, res) => {
    const order = await getOrderById(req.params.id);
    if (!order) {
      return res.status(404).json({ message: 'order not found' });
    }
    return res.status(200).json(order);
  }
);

app.get('/api/orders', async (_req, res) => {
  return res.status(200).json(await listOrders());
});

app.get('/', (_req, res) => {
  res.status(200).json({
    service: 'orders',
    message: 'ticket-selling orders service is running',
    integrations: {
      sqsOrdersWorkerQueueEnabled: Boolean(sqsClient && sqsOrdersServiceQueueUrl),
      sqsEventPublishEnabled: Boolean(sqsClient && sqsQueueUrl),
      sqsExpirationPublishEnabled: Boolean(sqsClient && sqsExpirationQueueUrl),
      sqsWorkerEnabled: Boolean(enableOrdersWorker && sqsClient && sqsOrdersServiceQueueUrl),
      redisPersistenceEnabled: Boolean(redisClient),
      recentJobs: orderJobs.length
    }
  });
});

app.get('/api/orders/jobs', (_req, res) => {
  res.status(200).json(orderJobs);
});

app.listen(port, () => {
  console.log('orders service listening on port ' + port);
  startOrdersWorker().catch((err) => {
    console.error('orders worker failed to start', err);
  });
});
