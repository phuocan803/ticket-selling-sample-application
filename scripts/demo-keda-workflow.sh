#!/bin/bash

###############################################################################
# KEDA SQS Producer Demo Script
# 
# Orchestrates a coordinated workflow across all 6 ticket-selling services:
# 1. Auth Service     → Publishes user.signup, user.signin events to SQS_AUTH_QUEUE
# 2. Client Service   → Publishes client.activity events to SQS_CLIENT_QUEUE
# 3. Tickets Service  → Publishes ticket.created, ticket.updated events to SQS_TICKETS_QUEUE
# 4. Orders Service   → Publishes order.accepted events to SQS_ORDERS_SERVICE_QUEUE
# 5. Payments Service → Already produces payment.created events
# 6. Expiration Service → Already produces order.expired events
#
# Usage:
#   ./demo-keda-workflow.sh [environment] [duration] [iterations]
#
# Arguments:
#   environment  - "local" (docker-compose) or "cluster" (EKS) [default: local]
#   duration     - Run duration in seconds or "once" for single iteration [default: 60]
#   iterations   - Number of workflow cycles (ignored if duration specified) [default: 1]
#
# Examples:
#   ./demo-keda-workflow.sh local              # Run 1 cycle on docker-compose
#   ./demo-keda-workflow.sh local once 1       # Run until stopped
#   ./demo-keda-workflow.sh local 300          # Run for 5 minutes on docker-compose
#   ./demo-keda-workflow.sh cluster 600 10     # Run 10 cycles on EKS cluster
#
# Prerequisites:
#   - Services running (docker-compose up OR deployed on EKS)
#   - curl installed
#   - jq installed (optional, for pretty-printing JSON)
#
###############################################################################

set -e

# Configuration
ENVIRONMENT="${1:-local}"
DURATION="${2:-60}"
ITERATIONS="${3:-1}"

# Service URLs (configure based on environment)
if [[ "$ENVIRONMENT" == "cluster" ]]; then
  AUTH_URL="${AUTH_SERVICE_URL:-http://auth-service:3001}"
  CLIENT_URL="${CLIENT_SERVICE_URL:-http://client-service:3000}"
  TICKETS_URL="${TICKETS_SERVICE_URL:-http://tickets-service:3002}"
  ORDERS_URL="${ORDERS_SERVICE_URL:-http://orders-service:3003}"
  PAYMENTS_URL="${PAYMENTS_SERVICE_URL:-http://payments-service:3004}"
else
  # Local docker-compose defaults
  AUTH_URL="${AUTH_SERVICE_URL:-http://localhost:3001}"
  CLIENT_URL="${CLIENT_SERVICE_URL:-http://localhost:3000}"
  TICKETS_URL="${TICKETS_SERVICE_URL:-http://localhost:3002}"
  ORDERS_URL="${ORDERS_SERVICE_URL:-http://localhost:3003}"
  PAYMENTS_URL="${PAYMENTS_SERVICE_URL:-http://localhost:3004}"
fi

# Demo data
USER_EMAIL="demo-$(date +%s)@example.com"
USER_PASSWORD="DemoPass123!"
TICKET_NAME="Demo Event $(date +%s)"
TICKET_PRICE=99.99

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

###############################################################################
# Helper Functions
###############################################################################

log_header() {
  echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${BLUE}$1${NC}"
  echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

log_step() {
  echo -e "${YELLOW}▶ $1${NC}"
}

log_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

log_error() {
  echo -e "${RED}✗ $1${NC}"
}

log_info() {
  echo -e "  $1"
}

# Check if service is reachable
check_service() {
  local service_name="$1"
  local url="$2"
  
  if curl -s -f "$url/health" > /dev/null 2>&1 || curl -s -f "$url/api/health" > /dev/null 2>&1; then
    log_success "$service_name is reachable"
    return 0
  else
    log_error "$service_name is NOT reachable at $url"
    return 1
  fi
}

# Make HTTP request with error handling
call_api() {
  local method="$1"
  local url="$2"
  local data="$3"
  local description="$4"
  
  log_step "$description"
  
  if [[ "$method" == "POST" ]] || [[ "$method" == "PUT" ]]; then
    local response=$(curl -s -w "\n%{http_code}" -X "$method" \
      -H "Content-Type: application/json" \
      -d "$data" \
      "$url" 2>/dev/null)
  else
    local response=$(curl -s -w "\n%{http_code}" -X "$method" "$url" 2>/dev/null)
  fi
  
  local http_code=$(echo "$response" | tail -n1)
  local body=$(echo "$response" | head -n-1)
  
  if [[ "$http_code" =~ ^[2][0-9][0-9]$ ]]; then
    log_success "HTTP $http_code"
    if [[ -n "$body" ]] && command -v jq &> /dev/null; then
      echo "$body" | jq '.' 2>/dev/null | sed 's/^/    /'
    elif [[ -n "$body" ]]; then
      log_info "Response: $body"
    fi
    echo "$body"
    return 0
  else
    log_error "HTTP $http_code"
    if [[ -n "$body" ]]; then
      log_info "Error: $body"
    fi
    return 1
  fi
}

###############################################################################
# Workflow Functions
###############################################################################

auth_workflow() {
  local workflow_id="$1"
  log_header "AUTH SERVICE WORKFLOW #$workflow_id"
  
  # Signup
  local user_data='{"email":"'$USER_EMAIL'","password":"'$USER_PASSWORD'"}'
  local signup_response=$(call_api POST "$AUTH_URL/api/auth/signup" "$user_data" "User Signup (publish auth event)")
  
  if [[ -z "$signup_response" ]]; then
    log_error "Signup failed, skipping signin"
    return 1
  fi
  
  sleep 0.5
  
  # Signin
  local signin_response=$(call_api POST "$AUTH_URL/api/auth/signin" "$user_data" "User Signin (publish auth event)")
  
  # Check integrations status
  call_api GET "$AUTH_URL/api/integrations" "" "Check Auth integrations (verify SQS producer enabled)" > /dev/null || true
  
  log_success "Auth workflow complete"
  sleep 1
}

client_workflow() {
  local workflow_id="$1"
  log_header "CLIENT SERVICE WORKFLOW #$workflow_id"
  
  # Page view event
  local client_event='{"action":"page_view","source":"demo-script","metadata":{"page":"/","timestamp":"'$(date -Iseconds)'"}}'
  call_api POST "$CLIENT_URL/api/client/events" "$client_event" "Client Page View Event (publish client event)" > /dev/null
  
  sleep 0.5
  
  # Click event
  client_event='{"action":"button_click","source":"demo-script","metadata":{"button":"buy-now","eventId":"'$(uuidgen 2>/dev/null || echo "event-$(date +%s%N)")'"}'
  call_api POST "$CLIENT_URL/api/client/events" "$client_event" "Client Button Click Event (publish client event)" > /dev/null
  
  sleep 0.5
  
  # Checkout event
  client_event='{"action":"checkout_start","source":"demo-script","metadata":{"cartValue":299.99}}'
  call_api POST "$CLIENT_URL/api/client/events" "$client_event" "Client Checkout Start Event (publish client event)" > /dev/null
  
  # View recent events
  call_api GET "$CLIENT_URL/api/client/events" "" "Retrieve Client Events History" > /dev/null
  
  log_success "Client workflow complete"
  sleep 1
}

tickets_workflow() {
  local workflow_id="$1"
  log_header "TICKETS SERVICE WORKFLOW #$workflow_id"
  
  # Create ticket
  local ticket_data='{"name":"'$TICKET_NAME'","location":"Demo Venue","price":'$TICKET_PRICE'}'
  local create_response=$(call_api POST "$TICKETS_URL/api/tickets" "$ticket_data" "Create Ticket (publish ticket.created event)")
  
  if [[ -z "$create_response" ]]; then
    log_error "Ticket creation failed, skipping update"
    return 1
  fi
  
  # Extract ticket ID from response (assumes response is valid JSON with id field)
  local ticket_id=""
  if command -v jq &> /dev/null; then
    ticket_id=$(echo "$create_response" | jq -r '.id // .ticketId // empty' 2>/dev/null)
  fi
  
  if [[ -z "$ticket_id" ]]; then
    log_info "Could not extract ticket ID, skipping update"
    log_success "Tickets workflow complete (create only)"
    sleep 1
    return 0
  fi
  
  sleep 0.5
  
  # Update ticket
  local update_data='{"name":"'$TICKET_NAME' - Updated","price":'$((${TICKET_PRICE%.*} + 10))'.99}'
  call_api PUT "$TICKETS_URL/api/tickets/$ticket_id" "$update_data" "Update Ticket (publish ticket.updated event)" > /dev/null
  
  # View tickets
  call_api GET "$TICKETS_URL/api/tickets" "" "List All Tickets" > /dev/null
  
  log_success "Tickets workflow complete"
  sleep 1
}

orders_workflow() {
  local workflow_id="$1"
  log_header "ORDERS SERVICE WORKFLOW #$workflow_id"
  
  # Create order
  local order_data='{"customerId":"customer-'$(date +%s)'","items":[{"productId":"TICKET-001","quantity":2,"price":"'$TICKET_PRICE'"}],"totalAmount":'$((${TICKET_PRICE%.*} * 2))'.98}'
  local create_response=$(call_api POST "$ORDERS_URL/api/orders" "$order_data" "Create Order (publish order.accepted event)")
  
  if [[ -z "$create_response" ]]; then
    log_error "Order creation failed"
    return 1
  fi
  
  # Extract order ID from response
  local order_id=""
  if command -v jq &> /dev/null; then
    order_id=$(echo "$create_response" | jq -r '.id // .orderId // empty' 2>/dev/null)
  fi
  
  if [[ -n "$order_id" ]]; then
    sleep 0.5
    # View order details
    call_api GET "$ORDERS_URL/api/orders/$order_id" "" "Retrieve Order Details" > /dev/null
  fi
  
  log_success "Orders workflow complete"
  sleep 1
}

payments_workflow() {
  local workflow_id="$1"
  log_header "PAYMENTS SERVICE WORKFLOW #$workflow_id"
  
  # Create payment
  local payment_data='{"orderId":"order-'$(date +%s)'","customerId":"customer-'$(date +%s)'","amount":199.98,"currency":"USD","status":"pending"}'
  call_api POST "$PAYMENTS_URL/api/payments" "$payment_data" "Create Payment (existing producer)" > /dev/null
  
  log_success "Payments workflow complete"
  sleep 1
}

check_integrations() {
  log_header "CHECKING ALL INTEGRATIONS"
  
  echo ""
  log_step "Auth Service Integrations"
  call_api GET "$AUTH_URL/api/integrations" "" "  Check SQS producer status" > /dev/null || true
  
  echo ""
  log_step "Client Service Integrations"
  call_api GET "$CLIENT_URL/api/integrations" "" "  Check SQS producer status" > /dev/null || true
  
  echo ""
  log_step "Tickets Service Integrations"
  call_api GET "$TICKETS_URL/api/integrations" "" "  Check SQS producer status" > /dev/null || true
  
  echo ""
  log_step "Orders Service Integrations"
  call_api GET "$ORDERS_URL/api/integrations" "" "  Check SQS producer status" > /dev/null || true
  
  echo ""
  log_step "Payments Service Integrations"
  call_api GET "$PAYMENTS_URL/api/integrations" "" "  Check SQS producer status" > /dev/null || true
  
  log_success "Integration checks complete"
}

###############################################################################
# Main Execution
###############################################################################

main() {
  local start_time=$(date +%s)
  
  log_header "KEDA SQS PRODUCER DEMO"
  log_info "Environment: $ENVIRONMENT"
  log_info "Duration: $DURATION (iterations: $ITERATIONS)"
  log_info "Start: $(date)"
  echo ""
  
  # Verify prerequisites
  if ! command -v curl &> /dev/null; then
    log_error "curl is not installed"
    exit 1
  fi
  
  # Check all services
  log_header "HEALTH CHECK"
  check_service "Auth Service" "$AUTH_URL"
  check_service "Client Service" "$CLIENT_URL"
  check_service "Tickets Service" "$TICKETS_URL"
  check_service "Orders Service" "$ORDERS_URL"
  check_service "Payments Service" "$PAYMENTS_URL"
  echo ""
  
  # Check integrations
  check_integrations
  echo ""
  
  # Run workflows
  local iteration=1
  
  if [[ "$DURATION" == "once" ]]; then
    # Run until interrupted
    log_header "RUNNING CONTINUOUS WORKFLOW (Press Ctrl+C to stop)"
    echo ""
    
    iteration=1
    while true; do
      log_header "ITERATION #$iteration"
      auth_workflow "$iteration"
      client_workflow "$iteration"
      tickets_workflow "$iteration"
      orders_workflow "$iteration"
      payments_workflow "$iteration"
      
      iteration=$((iteration + 1))
      
      log_info "Next iteration in 5 seconds... (Ctrl+C to stop)"
      sleep 5
      echo ""
    done
  elif [[ "$DURATION" =~ ^[0-9]+$ ]]; then
    # Run for specified duration
    local end_time=$((start_time + DURATION))
    
    while [[ $(date +%s) -lt $end_time ]]; do
      log_header "ITERATION #$iteration ($(( ($(date +%s) - start_time) ))s elapsed)"
      auth_workflow "$iteration"
      client_workflow "$iteration"
      tickets_workflow "$iteration"
      orders_workflow "$iteration"
      payments_workflow "$iteration"
      
      local elapsed=$(($(date +%s) - start_time))
      local remaining=$((DURATION - elapsed))
      
      if [[ $remaining -gt 0 ]]; then
        log_info "Next iteration in 5 seconds (${remaining}s remaining)..."
        sleep 5
      fi
      
      iteration=$((iteration + 1))
      echo ""
    done
  else
    # Run specified number of iterations
    for ((i = 1; i <= ITERATIONS; i++)); do
      log_header "ITERATION #$i / $ITERATIONS"
      auth_workflow "$i"
      client_workflow "$i"
      tickets_workflow "$i"
      orders_workflow "$i"
      payments_workflow "$i"
      
      if [[ $i -lt $ITERATIONS ]]; then
        log_info "Next iteration in 5 seconds..."
        sleep 5
      fi
      echo ""
    done
  fi
  
  # Final summary
  local end_time=$(date +%s)
  local total_duration=$((end_time - start_time))
  
  log_header "DEMO COMPLETE"
  log_info "Total Duration: ${total_duration}s"
  log_info "Iterations Completed: $((iteration - 1))"
  log_info "End: $(date)"
  echo ""
  
  # Instructions for monitoring
  log_header "MONITORING INSTRUCTIONS"
  echo ""
  log_step "If running on EKS with AMP, check KEDA scaler activity:"
  echo "  # Watch HPA scaling decisions"
  echo "  kubectl get hpa -A -w"
  echo ""
  log_step "Monitor SQS queue depths:"
  echo "  # List all queues and their message counts"
  echo "  aws sqs list-queues --region us-east-1"
  echo ""
  log_step "View KEDA scaler metrics:"
  echo "  # Check scaler status in Prometheus"
  echo "  kubectl port-forward -n custom-metrics svc/prometheus 9090:9090"
  echo "  # Then visit http://localhost:9090 and search for: keda_*"
  echo ""
  log_step "Monitor consumer logs:"
  echo "  # Watch all service pods processing messages"
  echo "  kubectl logs -f -l app=auth-worker -n default"
  echo "  kubectl logs -f -l app=tickets-worker -n default"
  echo "  kubectl logs -f -l app=orders-worker -n default"
  echo ""
  
  log_success "Demo workflow finished successfully!"
}

# Run main function
main
