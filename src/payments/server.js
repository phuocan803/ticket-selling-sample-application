const crypto = require('crypto');

const express = require('express');
const { body, param, query, validationResult } = require('express-validator');
const client = require('prom-client');
const { createClient } = require('redis');
const {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient
} = require('@aws-sdk/client-sqs');

const app = express();
const port = process.env.PORT || 3000;

const sqsPaymentEventsQueueUrl = process.env.SQS_PAYMENT_EVENTS_QUEUE_URL || '';
const sqsOrderEventsQueueUrl = process.env.SQS_ORDER_EVENTS_QUEUE_URL || '';
const sqsRegion = process.env.AWS_REGION || '';
const enableSqsConsumer = process.env.ENABLE_SQS_CONSUMER === 'true';
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);

const sqsClient = sqsRegion ? new SQSClient({ region: sqsRegion }) : null;
const register = new client.Registry();
const payments = new Map();
const paidByOrderId = new Map();
const redisUrl = process.env.REDIS_URL || '';
const redisPrefix = process.env.REDIS_PREFIX || 'payments';
const redisClient = redisUrl ? createClient({ url: redisUrl }) : null;
let redisConnectPromise = null;

client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the payments service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

if (redisClient) {
  redisClient.on('error', (err) => {
    console.error('payments redis error', err);
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

const paymentsHashKey = `${redisPrefix}:payments`;
const paidOrdersHashKey = `${redisPrefix}:paidByOrderId`;

const savePayment = async (payment) => {
  payments.set(payment.id, payment);
  paidByOrderId.set(payment.orderId, payment);

  if (!redisClient) {
    return;
  }

  await ensureRedisConnection();
  await redisClient.hSet(paymentsHashKey, payment.id, JSON.stringify(payment));
  await redisClient.hSet(paidOrdersHashKey, payment.orderId, JSON.stringify(payment));
};

const getPaymentById = async (id) => {
  if (!redisClient) {
    return payments.get(id) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.hGet(paymentsHashKey, id);
  return raw ? JSON.parse(raw) : null;
};

const getPaymentByOrderId = async (orderId) => {
  if (!redisClient) {
    return paidByOrderId.get(orderId) || null;
  }

  await ensureRedisConnection();
  const raw = await redisClient.hGet(paidOrdersHashKey, orderId);
  return raw ? JSON.parse(raw) : null;
};

const listPayments = async () => {
  if (!redisClient) {
    return Array.from(payments.values());
  }

  await ensureRedisConnection();
  const values = await redisClient.hVals(paymentsHashKey);
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

const publishPaymentCreated = async (payment) => {
  if (!sqsClient || !sqsPaymentEventsQueueUrl) {
    return;
  }

  const payload = {
    type: 'payment.created',
    data: payment,
    emittedAt: new Date().toISOString()
  };

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsPaymentEventsQueueUrl,
      MessageBody: JSON.stringify(payload)
    })
  );
};

const createPayment = async ({ orderId, amount, token = 'tok_demo', source = 'api' }) => {
  const existingPayment = await getPaymentByOrderId(orderId);
  if (existingPayment) {
    return existingPayment;
  }

  const payment = {
    id: crypto.randomUUID(),
    orderId,
    amount: Number(amount),
    status: 'paid',
    provider: 'stripe-simulated',
    token,
    source,
    createdAt: new Date().toISOString()
  };

  await savePayment(payment);
  await publishPaymentCreated(payment);
  return payment;
};

const parseOrderEvent = (messageBody) => {
  const raw = JSON.parse(messageBody);
  const data = raw && raw.data ? raw.data : raw;
  const orderId = data && (data.orderId || data.id);
  if (!data || !orderId) {
    return null;
  }
  if (!data.amount || Number(data.amount) <= 0) {
    return null;
  }
  return {
    orderId,
    amount: Number(data.amount),
    token: data.token || 'tok_sqs'
  };
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const startSqsConsumer = async () => {
  if (!enableSqsConsumer || !sqsClient || !sqsOrderEventsQueueUrl) {
    return;
  }

  console.log('payments SQS consumer enabled');

  while (true) {
    try {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: sqsOrderEventsQueueUrl,
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
          const paymentInput = parseOrderEvent(message.Body);
          if (paymentInput) {
            await createPayment({ ...paymentInput, source: 'sqs' });
          }

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsOrderEventsQueueUrl,
              ReceiptHandle: receiptHandle
            })
          );
        } catch (err) {
          console.error('failed to process SQS message', err);
        }
      }
    } catch (err) {
      console.error('SQS polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'payments', status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.post(
  '/api/payments',
  [
    body('orderId').isUUID().withMessage('orderId must be a valid UUID'),
    body('amount').isFloat({ min: 0.5 }).withMessage('amount must be greater than 0'),
    body('token').optional().trim().notEmpty().withMessage('token cannot be empty')
  ],
  requireValidRequest,
  async (req, res) => {
    try {
      const payment = await createPayment({
        orderId: req.body.orderId,
        amount: Number(req.body.amount),
        token: req.body.token || 'tok_demo',
        source: 'api'
      });
      return res.status(201).json(payment);
    } catch (err) {
      console.error('failed to publish payment.created event', err);
      return res.status(500).json({ message: 'failed to create payment' });
    }
  }
);

app.get(
  '/api/payments/:id',
  [param('id').isUUID().withMessage('id must be a valid UUID')],
  requireValidRequest,
  async (req, res) => {
    const payment = await getPaymentById(req.params.id);
    if (!payment) {
      return res.status(404).json({ message: 'payment not found' });
    }
    return res.status(200).json(payment);
  }
);

app.get(
  '/api/payments',
  [query('orderId').optional().isUUID().withMessage('orderId must be a valid UUID')],
  requireValidRequest,
  async (req, res) => {
    const orderId = req.query.orderId;
    const allPayments = await listPayments();
    if (!orderId) {
      return res.status(200).json(allPayments);
    }
    const list = allPayments.filter((it) => it.orderId === orderId);
    return res.status(200).json(list);
  }
);

app.get('/', (_req, res) => {
  res.status(200).json({
    service: 'payments',
    message: 'ticket-selling payments service is running',
    integrations: {
      sqsEventPublishEnabled: Boolean(sqsClient && sqsPaymentEventsQueueUrl),
      sqsConsumerEnabled: Boolean(enableSqsConsumer && sqsClient && sqsOrderEventsQueueUrl),
      redisPersistenceEnabled: Boolean(redisClient)
    }
  });
});

app.listen(port, () => {
  console.log('payments service listening on port ' + port);
  startSqsConsumer().catch((err) => {
    console.error('payments SQS consumer failed to start', err);
  });
});
// Payment integration with Stripe
// Payment service improvements based on code review
