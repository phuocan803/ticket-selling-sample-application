const express = require('express');
const client = require('prom-client');
const { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');

const app = express();
const port = process.env.PORT || 3000;
const sqsRegion = process.env.AWS_REGION || '';
const sqsQueueUrl = process.env.SQS_CLIENT_QUEUE_URL || '';
const authServiceUrl = process.env.AUTH_SERVICE_URL || 'http://auth:3000';
const ticketsServiceUrl = process.env.TICKETS_SERVICE_URL || 'http://tickets:3000';
const ordersServiceUrl = process.env.ORDERS_SERVICE_URL || 'http://orders:3000';
const paymentsServiceUrl = process.env.PAYMENTS_SERVICE_URL || 'http://payments:3000';
const expirationServiceUrl = process.env.EXPIRATION_SERVICE_URL || 'http://expiration:3000';
const documentDbUriByService = {
  auth: process.env.AUTH_MONGO_URI || '',
  tickets: process.env.TICKETS_MONGO_URI || '',
  orders: process.env.ORDERS_MONGO_URI || '',
  payments: process.env.PAYMENTS_MONGO_URI || ''
};
const enableWorker = process.env.ENABLE_CLIENT_WORKER === 'true';
const sqsMaxMessages = Number(process.env.SQS_MAX_MESSAGES || 5);
const sqsWaitTimeSeconds = Number(process.env.SQS_WAIT_TIME_SECONDS || 10);
const sqsVisibilityTimeout = Number(process.env.SQS_VISIBILITY_TIMEOUT || 30);
const sqsPollSleepMs = Number(process.env.SQS_POLL_SLEEP_MS || 2000);
const sqsClient = sqsRegion && sqsQueueUrl ? new SQSClient({ region: sqsRegion }) : null;
const clientJobs = [];
const clientEvents = [];

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: 'app_http_requests_total',
  help: 'Total HTTP requests handled by the client service',
  labelNames: ['method', 'route', 'status_code']
});

register.registerMetric(httpRequestsTotal);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const proxyRequest = async (req, res, method, targetBaseUrl, targetPath, body) => {
  try {
    const upstreamHeaders = {};
    if (body !== undefined) {
      upstreamHeaders['Content-Type'] = 'application/json';
    }
    if (req.headers.cookie) {
      upstreamHeaders.cookie = req.headers.cookie;
    }
    if (req.headers.authorization) {
      upstreamHeaders.authorization = req.headers.authorization;
    }

    const upstreamResponse = await fetch(`${targetBaseUrl}${targetPath}`, {
      method,
      headers: upstreamHeaders,
      body: body !== undefined ? JSON.stringify(body) : undefined
    });

    let setCookies = [];
    if (typeof upstreamResponse.headers.getSetCookie === 'function') {
      setCookies = upstreamResponse.headers.getSetCookie();
    } else {
      const setCookie = upstreamResponse.headers.get('set-cookie');
      if (setCookie) {
        setCookies = [setCookie];
      }
    }
    if (setCookies.length > 0) {
      res.setHeader('set-cookie', setCookies);
    }

    const upstreamText = await upstreamResponse.text();
    const contentType = upstreamResponse.headers.get('content-type') || '';
    const isJson = contentType.includes('application/json');

    if (isJson) {
      return res.status(upstreamResponse.status).type('application/json').send(upstreamText);
    }

    return res.status(upstreamResponse.status).json({
      message: 'upstream returned non-json response',
      raw: upstreamText
    });
  } catch (err) {
    console.error('proxy request failed', err);
    return res.status(502).json({ message: 'failed to reach upstream service' });
  }
};

const getServiceOverview = async (name, baseUrl) => {
  const documentDbConfigured = Boolean(documentDbUriByService[name]);
  try {
    const [healthResponse, rootResponse] = await Promise.all([
      fetch(`${baseUrl}/healthz`),
      fetch(`${baseUrl}/`)
    ]);

    let healthBody = { message: 'no json body' };
    let rootBody = { message: 'no json body' };

    try {
      healthBody = await healthResponse.json();
    } catch (_err) {
      // keep default fallback when endpoint does not return JSON
    }

    try {
      rootBody = await rootResponse.json();
    } catch (_err) {
      // keep default fallback when endpoint does not return JSON
    }

    const integrations = rootBody && rootBody.integrations ? rootBody.integrations : {};
    return {
      service: name,
      health: {
        statusCode: healthResponse.status,
        body: healthBody
      },
      integrations,
      dataLayer: {
        database: documentDbConfigured ? 'documentdb configured' : 'not configured in sample',
        cacheRedisEnabled: Boolean(integrations.redisPersistenceEnabled),
        documentDbConfigured,
        persistenceMode:
          integrations.redisPersistenceEnabled && documentDbConfigured
            ? 'redis + documentdb'
            : integrations.redisPersistenceEnabled
              ? 'redis'
              : documentDbConfigured
                ? 'documentdb'
                : 'in-memory fallback'
      }
    };
  } catch (err) {
    return {
      service: name,
      health: { statusCode: 502, body: { message: 'unreachable' } },
      integrations: {},
      dataLayer: {
        database: documentDbConfigured ? 'documentdb configured' : 'unknown',
        cacheRedisEnabled: false,
        documentDbConfigured,
        persistenceMode: documentDbConfigured ? 'documentdb (configured)' : 'unknown'
      },
      error: err.message
    };
  }
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

const startClientWorker = async () => {
  if (!enableWorker || !sqsClient || !sqsQueueUrl) {
    return;
  }

  console.log('client SQS worker enabled');

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
          clientJobs.unshift({
            id: message.MessageId,
            body: message.Body,
            processedAt: new Date().toISOString()
          });
          clientJobs.splice(20);

          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: sqsQueueUrl,
              ReceiptHandle: message.ReceiptHandle
            })
          );
        } catch (err) {
          console.error('client worker failed to process message', err);
        }
      }
    } catch (err) {
      console.error('client worker polling error', err);
      await sleep(sqsPollSleepMs);
    }
  }
};

const publishClientEvent = async (event) => {
  if (!sqsClient || !sqsQueueUrl) {
    return null;
  }

  const payload = {
    id: event.id,
    action: event.action,
    source: event.source,
    metadata: event.metadata,
    createdAt: event.createdAt
  };

  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: sqsQueueUrl,
      MessageBody: JSON.stringify({
        type: 'client.activity',
        data: payload,
        emittedAt: new Date().toISOString()
      })
    })
  );

  return payload;
};

app.get('/healthz', (_req, res) => {
  res.status(200).json({ service: 'client', status: 'ok' });
});

app.get('/', (_req, res) => {
  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ticket Selling on AWS</title>
  <style>
    :root {
      --page-bg: #eef2f7;
      --panel-bg: #ffffff;
      --panel-border: #d8e1ec;
      --text: #122033;
      --muted: #4c5d73;
      --primary: #0057b8;
      --primary-strong: #003f87;
      --input-bg: #f7f9fc;
      --output-bg: #06122b;
      --output-text: #d6e7ff;
      --guide-bg: #fff7e8;
      --guide-border: #ffd489;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "Segoe UI", "Noto Sans", Tahoma, sans-serif;
      background:
        radial-gradient(circle at 15% 10%, #dcecff 0, #dcecff 14%, transparent 40%),
        radial-gradient(circle at 90% 15%, #ffe8ca 0, #ffe8ca 12%, transparent 36%),
        var(--page-bg);
      min-height: 100vh;
      padding: 28px 16px;
    }

    .container {
      max-width: 1800px;
      margin: 0 auto;
      background: var(--panel-bg);
      border: 1px solid var(--panel-border);
      border-radius: 18px;
      padding: 28px 32px;
      box-shadow: 0 12px 36px rgba(8, 24, 48, 0.12);
    }

    h1 {
      margin: 0 0 8px;
      font-size: 2rem;
      letter-spacing: -0.02em;
    }

    .subtitle {
      margin: 0;
      color: var(--muted);
    }

    .layout {
      display: grid;
      grid-template-columns: 55% 45%;
      gap: 20px;
      margin-top: 20px;
    }

    /* Sidebar drawer */
    .drawer-toggle {
      position: fixed;
      right: 0;
      top: 50%;
      transform: translateY(-50%);
      z-index: 1000;
      writing-mode: vertical-rl;
      text-orientation: mixed;
      background: var(--primary);
      color: #fff;
      border: none;
      border-radius: 8px 0 0 8px;
      padding: 14px 8px;
      cursor: pointer;
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      box-shadow: -3px 0 12px rgba(0,0,0,0.15);
      transition: background 0.15s;
      width: auto;
    }
    .drawer-toggle:hover { background: var(--primary-strong); filter: none; }

    .drawer-overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.25);
      z-index: 998;
    }
    .drawer-overlay.open { display: block; }

    .drawer {
      position: fixed;
      top: 0;
      right: -380px;
      width: 360px;
      height: 100vh;
      background: #fff;
      border-left: 1px solid var(--panel-border);
      box-shadow: -8px 0 32px rgba(8,24,48,0.16);
      z-index: 999;
      display: flex;
      flex-direction: column;
      transition: right 0.28s cubic-bezier(0.4,0,0.2,1);
      padding: 20px 16px;
      overflow-y: auto;
    }
    .drawer.open { right: 0; }

    .drawer-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
    }
    .drawer-head h3 { margin: 0; font-size: 1rem; }
    .drawer-close {
      background: none;
      border: 1px solid #c8d3e1;
      border-radius: 8px;
      padding: 4px 10px;
      font-size: 0.82rem;
      cursor: pointer;
      color: var(--text);
      width: auto;
    }
    .drawer-close:hover { background: #f0f4fa; filter: none; }

    /* Flow stepper */
    .stepper {
      display: flex;
      align-items: center;
      gap: 0;
      margin: 20px 0 4px;
      background: #f4f7fb;
      border: 1px solid var(--panel-border);
      border-radius: 14px;
      padding: 14px 20px;
    }
    .step {
      display: flex;
      flex-direction: column;
      align-items: center;
      flex: 1;
      position: relative;
      gap: 6px;
    }
    .step:not(:last-child)::after {
      content: '';
      position: absolute;
      top: 15px;
      left: 50%;
      width: 100%;
      height: 2px;
      background: #d0dcea;
      z-index: 0;
    }
    .step.done:not(:last-child)::after { background: #34c06e; }
    .step-circle {
      width: 32px;
      height: 32px;
      border-radius: 50%;
      border: 2px solid #c0cedf;
      background: #fff;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.8rem;
      font-weight: 700;
      color: #7a92ab;
      z-index: 1;
      position: relative;
      transition: all 0.2s;
    }
    .step.active .step-circle {
      border-color: var(--primary);
      background: var(--primary);
      color: #fff;
    }
    .step.done .step-circle {
      border-color: #34c06e;
      background: #34c06e;
      color: #fff;
      font-size: 1rem;
    }
    .step-label {
      font-size: 0.75rem;
      font-weight: 600;
      color: #7a92ab;
      text-align: center;
      line-height: 1.2;
      white-space: nowrap;
    }
    .step.active .step-label { color: var(--primary); }
    .step.done .step-label { color: #1a7d43; }

    .panel {
      background: #fff;
      border: 1px solid var(--panel-border);
      border-radius: 14px;
      padding: 16px;
    }

    .section {
      margin-top: 14px;
      padding: 14px;
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      background: #fcfdff;
      transition: border-color 0.2s, background 0.2s, opacity 0.2s;
    }

    .section.step-active {
      border-color: var(--primary);
      background: #f0f6ff;
      box-shadow: 0 0 0 3px rgba(0,87,184,0.08);
    }

    .section.step-done {
      border-color: #34c06e;
      background: #f3fdf6;
    }

    .section.step-pending {
      opacity: 0.55;
    }

    .section h3 {
      margin: 0 0 6px;
      font-size: 1.05rem;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .section.step-done h3::after {
      content: '✓';
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 20px;
      height: 20px;
      border-radius: 50%;
      background: #34c06e;
      color: #fff;
      font-size: 0.75rem;
      font-weight: 700;
    }

    .section.step-active h3 {
      color: var(--primary);
    }

    .hint {
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 0.92rem;
    }

    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 10px;
    }

    .field label {
      display: block;
      font-size: 0.86rem;
      font-weight: 600;
      margin-bottom: 6px;
      color: #21354f;
    }

    input, button {
      width: 100%;
      border-radius: 10px;
      border: 1px solid #c8d3e1;
      padding: 10px 11px;
      font-size: 0.95rem;
    }

    input {
      background: var(--input-bg);
      color: var(--text);
    }

    input:focus {
      outline: 2px solid rgba(0, 87, 184, 0.2);
      border-color: var(--primary);
      background: #fff;
    }

    button {
      background: linear-gradient(180deg, var(--primary), var(--primary-strong));
      color: #fff;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: transform 0.08s ease, filter 0.2s ease;
    }

    button:hover { filter: brightness(1.06); }
    button:active { transform: translateY(1px); }

    .output-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 8px;
    }

    .status-pill {
      font-size: 0.8rem;
      font-weight: 700;
      color: #0a3b78;
      background: #d7ebff;
      border: 1px solid #9ec8f7;
      padding: 3px 8px;
      border-radius: 999px;
    }

    pre {
      margin: 0;
      background: var(--output-bg);
      color: var(--output-text);
      padding: 12px;
      border-radius: 10px;
      overflow: auto;
      max-height: 520px;
      min-height: 220px;
      line-height: 1.42;
      font-size: 0.87rem;
      border: 1px solid #1d325e;
      white-space: pre-wrap;
      word-break: break-all;
    }

    .guide {
      background: var(--guide-bg);
      border: 1px solid var(--guide-border);
      border-radius: 12px;
      padding: 12px;
    }

    .guide h3 {
      margin: 0 0 8px;
      font-size: 1rem;
    }

    .guide p {
      margin: 0 0 8px;
      color: #5e4f2e;
      font-size: 0.92rem;
    }

    .guide ol {
      margin: 0;
      padding-left: 18px;
      color: #3b2f16;
      font-size: 0.92rem;
      line-height: 1.5;
    }

    .small-note {
      margin-top: 10px;
      font-size: 0.84rem;
      color: #5a6c85;
    }

    .btn-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 8px;
    }

    .btn-grid button {
      padding: 9px;
      font-size: 0.86rem;
    }

    .infra-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      margin-top: 10px;
    }

    .infra-card {
      border: 1px solid #cad8ea;
      border-radius: 10px;
      padding: 9px;
      background: #f8fbff;
    }

    .infra-title {
      margin: 0 0 5px;
      font-size: 0.9rem;
      font-weight: 700;
    }

    .infra-meta {
      margin: 0;
      font-size: 0.8rem;
      color: #355173;
      line-height: 1.35;
    }

    .chip {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 999px;
      font-size: 0.76rem;
      font-weight: 700;
      margin-right: 4px;
      margin-bottom: 4px;
    }

    .chip-ok { background: #dff7e7; color: #155f31; border: 1px solid #98dbaf; }
    .chip-warn { background: #fff1cf; color: #8a5a00; border: 1px solid #f0c56c; }
    .chip-info { background: #e4efff; color: #184e99; border: 1px solid #acc8f5; }

    @media (max-width: 640px) {
      body { padding: 14px 10px; }
      .container { padding: 14px; border-radius: 12px; }
      .row { grid-template-columns: 1fr; }
      h1 { font-size: 1.5rem; }
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Ticket Selling</h1>

    <!-- Flow progress stepper -->
    <div class="stepper" id="stepper">
      <div class="step active" id="step-1">
        <div class="step-circle" id="step-circle-1">1</div>
        <div class="step-label">Create<br>Account</div>
      </div>
      <div class="step" id="step-2">
        <div class="step-circle" id="step-circle-2">2</div>
        <div class="step-label">Sign In</div>
      </div>
      <div class="step" id="step-3">
        <div class="step-circle" id="step-circle-3">3</div>
        <div class="step-label">Create<br>Ticket</div>
      </div>
      <div class="step" id="step-4">
        <div class="step-circle" id="step-circle-4">4</div>
        <div class="step-label">Create<br>Order</div>
      </div>
      <div class="step" id="step-5">
        <div class="step-circle" id="step-circle-5">5</div>
        <div class="step-label">Create<br>Payment</div>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <div class="section step-active" id="form-step-1">
          <h3>1) Create user</h3>
          <p class="hint">Create an account first to initialize the flow.</p>
          <div class="row">
            <div class="field">
              <label for="email">Email</label>
              <input id="email" placeholder="demo@example.com" value="demo@example.com" />
            </div>
            <div class="field">
              <label for="password">Password</label>
              <input id="password" placeholder="12345" value="12345" />
            </div>
          </div>
          <button onclick="signup()">Sign up</button>
        </div>

        <div class="section step-pending" id="form-step-2">
          <h3>2) Sign in</h3>
          <p class="hint">Sign in with the account you just created.</p>
          <div class="row">
            <div class="field">
              <label for="loginEmail">Login email</label>
              <input id="loginEmail" placeholder="demo@example.com" value="demo@example.com" />
            </div>
            <div class="field">
              <label for="loginPassword">Login password</label>
              <input id="loginPassword" placeholder="12345" value="12345" />
            </div>
          </div>
          <button onclick="signin()">Sign in</button>
        </div>

        <div class="section step-pending" id="form-step-3">
          <h3>3) Create ticket</h3>
          <p class="hint">Create a ticket product to purchase.</p>
          <div class="row">
            <div class="field">
              <label for="title">Ticket title</label>
              <input id="title" placeholder="Concert VIP" value="Concert VIP" />
            </div>
            <div class="field">
              <label for="price">Price</label>
              <input id="price" placeholder="99" value="99" />
            </div>
          </div>
          <button onclick="createTicket()">Create ticket</button>
        </div>

        <div class="section step-pending" id="form-step-4">
          <h3>4) Create order</h3>
          <p class="hint">Use ticketId from step 3. It can be auto-filled after success.</p>
          <div class="row">
            <div class="field">
              <label for="ticketId">Ticket ID</label>
              <input id="ticketId" placeholder="Paste ticket ID from output" />
            </div>
            <div class="field">
              <label for="quantity">Quantity</label>
              <input id="quantity" placeholder="1" value="1" />
            </div>
          </div>
          <button onclick="createOrder()">Create order</button>
        </div>

        <div class="section step-pending" id="form-step-5">
          <h3>5) Create payment</h3>
          <p class="hint">Use orderId from step 4. Amount can be auto-filled from order response.</p>
          <div class="row">
            <div class="field">
              <label for="orderId">Order ID</label>
              <input id="orderId" placeholder="Paste order ID from output" />
            </div>
            <div class="field">
              <label for="amount">Amount</label>
              <input id="amount" placeholder="10" value="10" />
            </div>
          </div>
          <button onclick="createPayment()">Create payment</button>
        </div>

        <div class="section">
          <h3>6) Explore all service functions</h3>
          <p class="hint">Quick actions to test all core APIs from every microservice.</p>
          <div class="btn-grid">
            <button onclick="signin()">Sign in</button>
            <button onclick="signout()">Sign out</button>
            <button onclick="currentUser()">Current user</button>
            <button onclick="listTickets()">List tickets</button>
            <button onclick="getTicket()">Get ticket by ID</button>
            <button onclick="updateTicket()">Update ticket</button>
            <button onclick="listOrders()">List orders</button>
            <button onclick="getOrder()">Get order by ID</button>
            <button onclick="listPayments()">List payments</button>
            <button onclick="getPayment()">Get payment by ID</button>
            <button onclick="listPaymentsByOrder()">Payments by orderId</button>
            <button onclick="listExpirations()">List expirations</button>
            <button onclick="getExpiration()">Get expiration by orderId</button>
            <button onclick="refreshInfra()">Refresh data/cache status</button>
          </div>
        </div>
      </div>

      <div class="panel">
        <div class="guide">
          <h3>How to use</h3>
          <p>Follow the exact order to avoid validation errors, then use explorer buttons for deeper checks.</p>
          <ol>
            <li>Click <strong>Sign up</strong> first.</li>
            <li>Create a ticket and copy ticket <strong>id</strong> if needed.</li>
            <li>Create an order using ticketId.</li>
            <li>Create payment using orderId and amount.</li>
            <li>Use <strong>Explore all service functions</strong> to test remaining APIs.</li>
          </ol>
          <p class="small-note">Tip: After each action, check Output. Status 2xx means success.</p>
        </div>

        <div class="section">
          <div class="output-head">
            <h3 style="margin: 0;">Output</h3>
            <span class="status-pill" id="status-pill">Ready</span>
          </div>
          <pre id="output">Ready</pre>
        </div>
      </div>

    </div>
  </div>

  <!-- Drawer toggle tab -->
  <button class="drawer-toggle" onclick="toggleInfra()">Data &amp; Cache Status</button>

  <!-- Drawer overlay -->
  <div class="drawer-overlay" id="drawer-overlay" onclick="closeInfra()"></div>

  <!-- Drawer panel -->
  <div class="drawer" id="infra-drawer">
    <div class="drawer-head">
      <h3>Data &amp; Cache Status</h3>
      <div style="display:flex;gap:6px;align-items:center;">
        <span class="status-pill" id="infra-pill"></span>
        <button class="drawer-close" onclick="fetchInfraData()">↻ Refresh</button>
        <button class="drawer-close" onclick="closeInfra()">✕ Close</button>
      </div>
    </div>
    <div class="infra-grid" id="infra-grid"></div>
  </div>

  <script>
    const output = document.getElementById('output');
    const statusPill = document.getElementById('status-pill');
    const infraPill = document.getElementById('infra-pill');
    const infraGrid = document.getElementById('infra-grid');

    const STEP_NAMES = ['signup','signin','createTicket','createOrder','createPayment'];
    const stepState = { signup: false, signin: false, createTicket: false, createOrder: false, createPayment: false };
    const stepMap = { signup: 1, signin: 2, createTicket: 3, createOrder: 4, createPayment: 5 };

    function markStep(name, success) {
      if (!success) return;
      stepState[name] = true;
      const idx = stepMap[name];
      // update top stepper
      const el = document.getElementById('step-' + idx);
      const circle = document.getElementById('step-circle-' + idx);
      el.classList.remove('active');
      el.classList.add('done');
      circle.textContent = '\u2713';
      // update form section for this step (if it has one)
      const formSec = document.getElementById('form-step-' + idx);
      if (formSec) {
        formSec.classList.remove('step-active', 'step-pending');
        formSec.classList.add('step-done');
      }
      // activate next step
      const next = idx + 1;
      if (next <= 5) {
        const nextEl = document.getElementById('step-' + next);
        if (!nextEl.classList.contains('done')) nextEl.classList.add('active');
        const nextForm = document.getElementById('form-step-' + next);
        if (nextForm) {
          nextForm.classList.remove('step-pending');
          nextForm.classList.add('step-active');
        }
      }
    }

    const show = async (response) => {
      const raw = await response.text();
      let body;
      try {
        body = raw ? JSON.parse(raw) : { message: 'empty body' };
      } catch (_e) {
        body = {
          message: 'no json body',
          raw: raw ? raw.slice(0, 500) : null
        };
      }

      if (response.status === 502 && body.message === 'no json body') {
        body.hint = 'upstream is likely scaled to zero (KEDA) or returning HTML gateway error';
      }

      output.textContent = JSON.stringify({ status: response.status, body }, null, 2);
      statusPill.textContent = 'HTTP ' + response.status;
      return body;
    };

    async function signup() {
      const res = await fetch('/api/users/signup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: document.getElementById('email').value, password: document.getElementById('password').value })
      });
      const body = await show(res);
      if (res.status >= 200 && res.status < 300) {
        document.getElementById('loginEmail').value = document.getElementById('email').value;
        document.getElementById('loginPassword').value = document.getElementById('password').value;
      }
      markStep('signup', res.status >= 200 && res.status < 300);
      return body;
    }

    async function createTicket() {
      const res = await fetch('/api/tickets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: document.getElementById('title').value, price: Number(document.getElementById('price').value) })
      });
      const body = await show(res);
      if (body && body.id) document.getElementById('ticketId').value = body.id;
      markStep('createTicket', res.status >= 200 && res.status < 300);
    }

    async function createOrder() {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticketId: document.getElementById('ticketId').value, quantity: Number(document.getElementById('quantity').value) })
      });
      const body = await show(res);
      if (body && body.id) {
        document.getElementById('orderId').value = body.id;
        document.getElementById('amount').value = body.amount || 10;
      }
      markStep('createOrder', res.status >= 200 && res.status < 300);
    }

    async function createPayment() {
      const res = await fetch('/api/payments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderId: document.getElementById('orderId').value, amount: Number(document.getElementById('amount').value), token: 'tok_demo' })
      });
      await show(res);
      markStep('createPayment', res.status >= 200 && res.status < 300);
    }

    async function signin() {
      const res = await fetch('/api/users/signin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: document.getElementById('loginEmail').value,
          password: document.getElementById('loginPassword').value
        })
      });
      await show(res);
      markStep('signin', res.status >= 200 && res.status < 300);
    }

    async function signout() {
      const res = await fetch('/api/users/signout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      await show(res);
    }

    async function currentUser() {
      const res = await fetch('/api/users/currentuser');
      await show(res);
    }

    async function listTickets() {
      const res = await fetch('/api/tickets');
      await show(res);
    }

    async function getTicket() {
      const id = document.getElementById('ticketId').value;
      const res = await fetch('/api/tickets/' + encodeURIComponent(id));
      await show(res);
    }

    async function updateTicket() {
      const id = document.getElementById('ticketId').value;
      const res = await fetch('/api/tickets/' + encodeURIComponent(id), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: document.getElementById('title').value,
          price: Number(document.getElementById('price').value),
          status: 'reserved'
        })
      });
      await show(res);
    }

    async function listOrders() {
      const res = await fetch('/api/orders');
      await show(res);
    }

    async function getOrder() {
      const id = document.getElementById('orderId').value;
      const res = await fetch('/api/orders/' + encodeURIComponent(id));
      await show(res);
    }

    async function listPayments() {
      const res = await fetch('/api/payments');
      await show(res);
    }

    async function getPayment() {
      const paymentId = prompt('Enter payment id');
      if (!paymentId) {
        return;
      }
      const res = await fetch('/api/payments/' + encodeURIComponent(paymentId));
      await show(res);
    }

    async function listPaymentsByOrder() {
      const id = document.getElementById('orderId').value;
      const res = await fetch('/api/payments?orderId=' + encodeURIComponent(id));
      await show(res);
    }

    async function listExpirations() {
      const res = await fetch('/api/expirations');
      await show(res);
    }

    async function getExpiration() {
      const id = document.getElementById('orderId').value;
      const res = await fetch('/api/expirations/' + encodeURIComponent(id));
      await show(res);
    }

    function renderInfraCard(service) {
      const statusCode = service.health ? service.health.statusCode : 0;
      const isHealthy = statusCode >= 200 && statusCode < 300;
      const isScaledToZero = statusCode === 502 || statusCode === 503;
      const redisEnabled = service.dataLayer && service.dataLayer.cacheRedisEnabled;
      const documentDbConfigured = service.dataLayer && service.dataLayer.documentDbConfigured;
      const persistenceMode = service.dataLayer ? service.dataLayer.persistenceMode : 'unknown';

      let statusLabel, statusClass;
      if (isHealthy) {
        statusLabel = 'healthy'; statusClass = 'chip-ok';
      } else if (isScaledToZero) {
        statusLabel = 'scaled to zero'; statusClass = 'chip-info';
      } else {
        statusLabel = 'unhealthy'; statusClass = 'chip-warn';
      }

      const redisLabel = redisEnabled ? 'redis enabled' : (isScaledToZero ? 'redis configured' : 'redis disabled');
      const redisClass = (redisEnabled || isScaledToZero) ? 'chip-info' : 'chip-warn';
      const documentDbLabel = documentDbConfigured ? 'documentdb configured' : 'documentdb missing';
      const documentDbClass = documentDbConfigured ? 'chip-info' : 'chip-warn';
      const persistLabel = isScaledToZero
        ? (documentDbConfigured ? 'redis + documentdb (configured)' : 'redis configured, documentdb missing')
        : persistenceMode;

      return '<div class="infra-card">'
        + '<p class="infra-title">' + service.service + '</p>'
        + '<div>'
        + '<span class="chip ' + statusClass + '">' + statusLabel + '</span>'
        + '<span class="chip ' + redisClass + '">' + redisLabel + '</span>'
        + '<span class="chip ' + documentDbClass + '">' + documentDbLabel + '</span>'
        + '</div>'
        + '<p class="infra-meta">persistence: ' + persistLabel + '</p>'
        + '<p class="infra-meta">health status: HTTP ' + (statusCode || 'n/a') + '</p>'
        + '</div>';
    }

    async function refreshInfra() {
      toggleInfra();
    }

    async function fetchInfraData() {
      infraPill.textContent = 'Loading...';
      const res = await fetch('/api/system/overview');
      const payload = await res.json().catch(() => null);
      if (!payload || !payload.services) {
        infraPill.textContent = 'Error';
        infraGrid.innerHTML = '<div class="infra-card"><p class="infra-meta">Failed to load</p></div>';
        return;
      }
      infraPill.textContent = payload.summary.healthyServices + '/' + payload.summary.serviceCount + ' healthy';
      infraGrid.innerHTML = payload.services.map(renderInfraCard).join('');
    }

    let infraLoaded = false;
    const infraDrawer = document.getElementById('infra-drawer');
    const drawerOverlay = document.getElementById('drawer-overlay');

    function openInfra() {
      infraDrawer.classList.add('open');
      drawerOverlay.classList.add('open');
      if (!infraLoaded) { infraLoaded = true; fetchInfraData(); }
    }

    function closeInfra() {
      infraDrawer.classList.remove('open');
      drawerOverlay.classList.remove('open');
    }

    function toggleInfra() {
      if (infraDrawer.classList.contains('open')) { closeInfra(); } else { openInfra(); }
    }
  </script>
</body>
</html>`;

  res.status(200).type('html').send(html);
});

app.get('/api/client/status', (_req, res) => {
  res.status(200).json({
    service: 'client',
    message: 'ticket-selling-sample-application client service is running',
    integrations: {
      sqsEventPublishEnabled: Boolean(sqsClient && sqsQueueUrl),
      sqsWorkerEnabled: Boolean(enableWorker && sqsClient && sqsQueueUrl),
      recentJobs: clientJobs.length
    }
  });
});

app.get('/api/client/jobs', (_req, res) => {
  res.status(200).json(clientJobs);
});

app.get('/api/client/events', (_req, res) => {
  res.status(200).json(clientEvents);
});

app.get('/api/system/overview', async (_req, res) => {
  const services = await Promise.all([
    getServiceOverview('client', `http://localhost:${port}`),
    getServiceOverview('auth', authServiceUrl),
    getServiceOverview('tickets', ticketsServiceUrl),
    getServiceOverview('orders', ordersServiceUrl),
    getServiceOverview('payments', paymentsServiceUrl),
    getServiceOverview('expiration', expirationServiceUrl)
  ]);

  return res.status(200).json({
    summary: {
      serviceCount: services.length,
      healthyServices: services.filter((it) => it.health.statusCode >= 200 && it.health.statusCode < 300).length,
      redisEnabledServices: services.filter((it) => it.dataLayer.cacheRedisEnabled).length,
      documentDbConfiguredServices: services.filter((it) => it.dataLayer.documentDbConfigured).length,
      note: 'Sample app uses Redis optional cache and DocumentDB URIs from config for service persistence visibility'
    },
    services
  });
});

app.post('/api/users/signup', async (req, res) => {
  return proxyRequest(req, res, 'POST', authServiceUrl, '/api/users/signup', req.body);
});

app.post('/api/users/signin', async (req, res) => {
  return proxyRequest(req, res, 'POST', authServiceUrl, '/api/users/signin', req.body);
});

app.post('/api/users/signout', async (req, res) => {
  return proxyRequest(req, res, 'POST', authServiceUrl, '/api/users/signout', req.body || {});
});

app.get('/api/users/currentuser', async (req, res) => {
  return proxyRequest(req, res, 'GET', authServiceUrl, '/api/users/currentuser');
});

app.post('/api/tickets', async (req, res) => {
  return proxyRequest(req, res, 'POST', ticketsServiceUrl, '/api/tickets', req.body);
});

app.get('/api/tickets', async (req, res) => {
  return proxyRequest(req, res, 'GET', ticketsServiceUrl, '/api/tickets');
});

app.get('/api/tickets/:id', async (req, res) => {
  return proxyRequest(req, res, 'GET', ticketsServiceUrl, `/api/tickets/${req.params.id}`);
});

app.put('/api/tickets/:id', async (req, res) => {
  return proxyRequest(req, res, 'PUT', ticketsServiceUrl, `/api/tickets/${req.params.id}`, req.body);
});

app.post('/api/orders', async (req, res) => {
  return proxyRequest(req, res, 'POST', ordersServiceUrl, '/api/orders', req.body);
});

app.get('/api/orders', async (req, res) => {
  return proxyRequest(req, res, 'GET', ordersServiceUrl, '/api/orders');
});

app.get('/api/orders/:id', async (req, res) => {
  return proxyRequest(req, res, 'GET', ordersServiceUrl, `/api/orders/${req.params.id}`);
});

app.post('/api/payments', async (req, res) => {
  return proxyRequest(req, res, 'POST', paymentsServiceUrl, '/api/payments', req.body);
});

app.get('/api/payments', async (req, res) => {
  const orderId = req.query.orderId;
  const path = orderId ? `/api/payments?orderId=${encodeURIComponent(orderId)}` : '/api/payments';
  return proxyRequest(req, res, 'GET', paymentsServiceUrl, path);
});

app.get('/api/payments/:id', async (req, res) => {
  return proxyRequest(req, res, 'GET', paymentsServiceUrl, `/api/payments/${req.params.id}`);
});

app.get('/api/expirations', async (req, res) => {
  return proxyRequest(req, res, 'GET', expirationServiceUrl, '/api/expirations');
});

app.get('/api/expirations/:orderId', async (req, res) => {
  return proxyRequest(req, res, 'GET', expirationServiceUrl, `/api/expirations/${req.params.orderId}`);
});

app.post('/api/client/events', async (req, res) => {
  const action = typeof req.body.action === 'string' && req.body.action.trim()
    ? req.body.action.trim()
    : 'page.view';

  const event = {
    id: req.body.id || String(Date.now()),
    action,
    source: typeof req.body.source === 'string' && req.body.source.trim() ? req.body.source.trim() : 'api',
    metadata: req.body.metadata && typeof req.body.metadata === 'object' ? req.body.metadata : {},
    createdAt: new Date().toISOString()
  };

  try {
    const publishedEvent = await publishClientEvent(event);
    clientEvents.unshift(publishedEvent);
    clientEvents.splice(20);
    return res.status(202).json(publishedEvent);
  } catch (err) {
    console.error('failed to publish client activity event', err);
    return res.status(500).json({ message: 'failed to publish client event' });
  }
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.listen(port, () => {
  console.log('client service listening on port ' + port);
  startClientWorker().catch((err) => {
    console.error('client worker failed to start', err);
  });
});
// UI improvements and styling
// Responsive design improvements
// Modern UI component library with Tailwind
