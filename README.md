# Ticket Selling Sample Application

This repository contains the source code for the **Ticket Selling Microservices Application**. It implements an event-driven ticket reservation and purchasing architecture built with Node.js and Express.

---

## Architecture & Microservices

The application is decomposed into six independent microservices:

| Service | Technology | Description |
| :--- | :--- | :--- |
| **auth** | Node.js / Express | User registration, authentication, and JWT token issuing |
| **client** | Node.js / Express | Frontend client application and API routing gateway |
| **tickets** | Node.js / MongoDB | Ticket inventory, seat mapping, and event management |
| **orders** | Node.js / MongoDB | Order creation and SQS event publishing (`ticket-orders-queue`) |
| **payments** | Node.js / Express | Asynchronous payment processing (SQS event consumer) |
| **expiration** | Node.js / Express | Ticket reservation expiration handler (SQS event consumer) |
| **load-generator** | Node.js / AWS SDK | SQS message publisher for generating synthetic workload |

---

## Repository Structure

```text
ticket-selling-sample-application/
├── .github/workflows/
│   └── 02-build-push.yml           # CI/CD workflow to build and push images to ECR
├── docs/                           # Feature specifications and system diagrams
├── samples/
│   └── data/tickets.json           # Sample seed dataset for event tickets
├── scripts/                        # Local build, e2e test, and compose scripts
├── src/                            # Microservices source code
│   ├── app/                        # Helmfile and docker-compose configurations
│   ├── auth/                       # Auth service source & Dockerfile
│   ├── client/                     # Client service source & Dockerfile
│   ├── expiration/                 # Expiration service source & Dockerfile
│   ├── load-generator/             # SQS load publishing script
│   ├── orders/                     # Orders service source & Dockerfile
│   ├── payments/                   # Payments service source & Dockerfile
│   └── tickets/                    # Tickets service source & Dockerfile
├── Dockerfile.prod                 # Production base container manifest
├── docker-compose.prod.yml         # Production multi-container orchestration
└── package.json                    # Workspace dependencies and scripts
```

---

## Local Development

### Prerequisites
- Node.js (v18+)
- Docker Desktop or Docker Engine with Docker Compose CLI

### Running with Docker Compose
To launch all services and MongoDB instances locally:

```bash
docker-compose -f docker-compose.prod.yml up --build
```

### Running Individual Services
Navigate to any service directory under `src/`:

```bash
cd src/auth
npm install
npm run dev
```

---

## CI/CD Pipeline

The GitHub Actions workflow defined in `.github/workflows/02-build-push.yml` automatically builds multi-stage Docker images for each microservice and pushes them to Amazon ECR upon changes to `src/**`.

---

## Related Repositories

- **[`ticket-selling-keda`](https://github.com/phuocan803/ticket-selling-keda)**: EKS cluster IaC (Terraform), KEDA ScaledObjects, ADOT Collector, Prometheus/SigV4 monitoring, load tests (k6/Locust), and HPA baseline.
- **[`ticket-selling-dev-setup`](https://github.com/phuocan803/ticket-selling-dev-setup)**: Local workstation setup guide, CLI installation scripts, and Rancher GUI console.
