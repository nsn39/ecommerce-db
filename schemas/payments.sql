-- 06_db_project_payments.sql
-- PostgreSQL 15+ | E-commerce database project
-- Requires scripts 01 (identity) and 05 (orders), including their prerequisites.
-- Execute once against a database where these tables do not yet exist.
-- Store only provider tokens and non-sensitive card metadata; never PANs or CVVs.
BEGIN;
CREATE SCHEMA IF NOT EXISTS db_project_payments;

-- 1. External payment processors.
CREATE TABLE db_project_payments.payment_providers (
    provider_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_code VARCHAR(40) NOT NULL UNIQUE,
    provider_name VARCHAR(120) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','inactive','maintenance')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tokenized customer payment methods.
CREATE TABLE db_project_payments.payment_methods (
    payment_method_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    provider_id BIGINT NOT NULL REFERENCES db_project_payments.payment_providers(provider_id) ON DELETE RESTRICT,
    method_type VARCHAR(20) NOT NULL CHECK (method_type IN ('card','bank_account','wallet')),
    provider_method_token TEXT NOT NULL,
    card_brand VARCHAR(30),
    card_last4 CHAR(4),
    card_exp_month SMALLINT CHECK (card_exp_month BETWEEN 1 AND 12),
    card_exp_year SMALLINT CHECK (card_exp_year BETWEEN 2000 AND 9999),
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','expired','removed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider_id, provider_method_token),
    UNIQUE (customer_id, payment_method_id),
    CHECK (card_last4 IS NULL OR card_last4 ~ '^[0-9]{4}$'),
    CHECK (method_type = 'card' OR (card_brand IS NULL AND card_last4 IS NULL AND card_exp_month IS NULL AND card_exp_year IS NULL)),
    CHECK (NOT is_default OR status = 'active')
);
CREATE INDEX idx_payment_methods_customer ON db_project_payments.payment_methods(customer_id);
CREATE UNIQUE INDEX uq_payment_methods_default_per_customer
    ON db_project_payments.payment_methods(customer_id) WHERE is_default;

-- 3. Intent to collect payment for an order; one order may have multiple attempts.
CREATE TABLE db_project_payments.payment_intents (
    payment_intent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    provider_id BIGINT NOT NULL REFERENCES db_project_payments.payment_providers(provider_id) ON DELETE RESTRICT,
    payment_method_id BIGINT,
    idempotency_key VARCHAR(128) NOT NULL,
    provider_intent_reference VARCHAR(255),
    amount NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    currency_code CHAR(3) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'created'
        CHECK (status IN ('created','requires_action','authorized','partially_captured','captured','cancelled','failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider_id, idempotency_key),
    UNIQUE (provider_id, provider_intent_reference),
    UNIQUE (payment_intent_id, provider_id),
    FOREIGN KEY (customer_id, payment_method_id)
        REFERENCES db_project_payments.payment_methods(customer_id, payment_method_id) ON DELETE RESTRICT
);
CREATE INDEX idx_payment_intents_order ON db_project_payments.payment_intents(order_id, created_at DESC);
CREATE INDEX idx_payment_intents_status ON db_project_payments.payment_intents(status, created_at);

-- 4. Provider transaction events (authorization, capture, void, refund).
CREATE TABLE db_project_payments.payment_transactions (
    payment_transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_intent_id BIGINT NOT NULL REFERENCES db_project_payments.payment_intents(payment_intent_id) ON DELETE RESTRICT,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('authorization','capture','void','refund')),
    status VARCHAR(16) NOT NULL CHECK (status IN ('pending','succeeded','failed')),
    amount NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    currency_code CHAR(3) NOT NULL,
    provider_transaction_reference VARCHAR(255),
    failure_code VARCHAR(80),
    failure_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    CHECK (completed_at IS NULL OR completed_at >= created_at),
    CHECK (status = 'pending' OR completed_at IS NOT NULL)
);
CREATE INDEX idx_payment_transactions_intent ON db_project_payments.payment_transactions(payment_intent_id, created_at DESC);
CREATE INDEX idx_payment_transactions_status ON db_project_payments.payment_transactions(status, created_at);

-- 5. Refund requests and outcomes; refunds can be partial.
CREATE TABLE db_project_payments.payment_refunds (
    refund_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_intent_id BIGINT NOT NULL REFERENCES db_project_payments.payment_intents(payment_intent_id) ON DELETE RESTRICT,
    refund_transaction_id BIGINT UNIQUE REFERENCES db_project_payments.payment_transactions(payment_transaction_id) ON DELETE RESTRICT,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    amount NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    currency_code CHAR(3) NOT NULL,
    reason VARCHAR(30) NOT NULL CHECK (reason IN ('customer_return','order_cancelled','duplicate','fraud','goodwill','other')),
    status VARCHAR(16) NOT NULL DEFAULT 'requested' CHECK (status IN ('requested','processing','succeeded','failed','cancelled')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    CHECK (completed_at IS NULL OR completed_at >= requested_at),
    CHECK (status NOT IN ('succeeded','failed','cancelled') OR completed_at IS NOT NULL)
);
CREATE INDEX idx_payment_refunds_intent ON db_project_payments.payment_refunds(payment_intent_id);

-- 6. Incoming processor webhook events for deduplication and replay.
CREATE TABLE db_project_payments.payment_webhook_events (
    webhook_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES db_project_payments.payment_providers(provider_id) ON DELETE RESTRICT,
    provider_event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    payload JSONB NOT NULL,
    processing_status VARCHAR(16) NOT NULL DEFAULT 'received'
        CHECK (processing_status IN ('received','processed','failed','ignored')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (provider_id, provider_event_id),
    CHECK (processed_at IS NULL OR processed_at >= received_at)
);
CREATE INDEX idx_payment_webhooks_pending ON db_project_payments.payment_webhook_events(received_at)
    WHERE processing_status IN ('received','failed');

-- 7. Reconciliation of local transaction records against provider settlements.
CREATE TABLE db_project_payments.payment_reconciliation (
    reconciliation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_transaction_id BIGINT NOT NULL UNIQUE
        REFERENCES db_project_payments.payment_transactions(payment_transaction_id) ON DELETE RESTRICT,
    provider_id BIGINT NOT NULL REFERENCES db_project_payments.payment_providers(provider_id) ON DELETE RESTRICT,
    settlement_reference VARCHAR(255),
    expected_amount NUMERIC(19,4) NOT NULL CHECK (expected_amount >= 0),
    settled_amount NUMERIC(19,4) CHECK (settled_amount >= 0),
    fee_amount NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (fee_amount >= 0),
    currency_code CHAR(3) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','matched','mismatch','missing')),
    reconciled_at TIMESTAMPTZ,
    notes TEXT,
    CHECK (status <> 'matched' OR settled_amount IS NOT NULL)
);
CREATE INDEX idx_payment_reconciliation_provider_status
    ON db_project_payments.payment_reconciliation(provider_id, status);

-- 8. Gift-card stored value accounts (one per issued gift card).
CREATE TABLE db_project_payments.gift_card_accounts (
    gift_card_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    card_code_hash CHAR(64) NOT NULL UNIQUE,
    owner_customer_id BIGINT REFERENCES db_project_identity.customers(customer_id) ON DELETE SET NULL,
    initial_balance NUMERIC(19,4) NOT NULL CHECK (initial_balance >= 0),
    current_balance NUMERIC(19,4) NOT NULL CHECK (current_balance >= 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended','expired','closed')),
    issued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    CHECK (expires_at IS NULL OR expires_at > issued_at)
);
CREATE INDEX idx_gift_cards_owner ON db_project_payments.gift_card_accounts(owner_customer_id);

-- 9. Gift-card ledger for credits and debits.
CREATE TABLE db_project_payments.gift_card_transactions (
    gift_card_transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gift_card_id BIGINT NOT NULL REFERENCES db_project_payments.gift_card_accounts(gift_card_id) ON DELETE RESTRICT,
    order_id BIGINT REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    transaction_type VARCHAR(16) NOT NULL CHECK (transaction_type IN ('issue','redeem','refund','adjustment')),
    amount NUMERIC(19,4) NOT NULL CHECK (amount <> 0),
    balance_after NUMERIC(19,4) NOT NULL CHECK (balance_after >= 0),
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    CHECK ((transaction_type = 'redeem' AND amount < 0)
        OR (transaction_type IN ('issue','refund') AND amount > 0)
        OR transaction_type = 'adjustment')
);
CREATE INDEX idx_gift_card_transactions_card_time
    ON db_project_payments.gift_card_transactions(gift_card_id, created_at DESC);

-- Business-level invariants requiring application/stored-procedure logic:
-- * Payment intent customer/currency/amount must match its order.
-- * Captures cannot exceed authorizations; refunds cannot exceed captures.
-- * Refund transaction must belong to the same intent and be of type 'refund'.
-- * Provider transaction references must be unique within each provider.
-- * Gift-card balances and ledger entries must change atomically under row locks.
-- * Provider calls should occur outside long-running database transactions.
COMMIT;
