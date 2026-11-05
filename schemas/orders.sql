-- 05_db_project_orders.sql
-- PostgreSQL 15+ | E-commerce database project
-- Requires identity, catalog, warehouse, and inventory schemas (scripts 01-04).
-- Run once against a database where db_project_orders does not already contain these tables.
BEGIN;
CREATE SCHEMA IF NOT EXISTS db_project_orders;

-- 1. Customer carts; historical/abandoned carts are retained for analysis.
CREATE TABLE db_project_orders.shopping_carts (
    cart_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','converted','abandoned','expired')),
    currency_code CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    CHECK (expires_at IS NULL OR expires_at > created_at)
);
CREATE INDEX idx_orders_carts_customer_status ON db_project_orders.shopping_carts(customer_id, status);
CREATE UNIQUE INDEX uq_orders_one_active_cart_per_customer
    ON db_project_orders.shopping_carts(customer_id) WHERE status = 'active';

-- 2. Items currently in each cart.
CREATE TABLE db_project_orders.cart_items (
    cart_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cart_id BIGINT NOT NULL REFERENCES db_project_orders.shopping_carts(cart_id) ON DELETE CASCADE,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    added_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (cart_id, variant_id)
);
CREATE INDEX idx_orders_cart_items_variant ON db_project_orders.cart_items(variant_id);

-- 3. Order headers; monetary totals are snapshots in order currency.
CREATE TABLE db_project_orders.orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_number VARCHAR(40) NOT NULL UNIQUE,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    cart_id BIGINT UNIQUE REFERENCES db_project_orders.shopping_carts(cart_id) ON DELETE SET NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'pending_payment'
        CHECK (status IN ('pending_payment','confirmed','processing','partially_shipped','shipped','delivered','cancelled','closed')),
    currency_code CHAR(3) NOT NULL DEFAULT 'USD',
    subtotal NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    discount_total NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (discount_total >= 0),
    tax_total NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (tax_total >= 0),
    shipping_total NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (shipping_total >= 0),
    grand_total NUMERIC(19,4) GENERATED ALWAYS AS (subtotal - discount_total + tax_total + shipping_total) STORED,
    placed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cancelled_at TIMESTAMPTZ,
    CHECK (discount_total <= subtotal),
    CHECK (cancelled_at IS NULL OR cancelled_at >= placed_at)
);
CREATE INDEX idx_orders_customer_placed ON db_project_orders.orders(customer_id, placed_at DESC);
CREATE INDEX idx_orders_status_placed ON db_project_orders.orders(status, placed_at DESC);
CREATE INDEX idx_orders_placed ON db_project_orders.orders(placed_at DESC);

-- 4. Immutable item-level purchase snapshots (price and tax at purchase time).
CREATE TABLE db_project_orders.order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    sku_snapshot VARCHAR(80) NOT NULL,
    product_name_snapshot VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(19,4) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    tax_amount NUMERIC(19,4) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    line_total NUMERIC(19,4) GENERATED ALWAYS AS
        (quantity * unit_price - discount_amount + tax_amount) STORED,
    CHECK (discount_amount <= quantity * unit_price),
    UNIQUE (order_id, line_number),
    UNIQUE (order_id, order_item_id)
);
CREATE INDEX idx_orders_items_variant ON db_project_orders.order_items(variant_id);

-- 5. Historical addresses: not foreign keyed to mutable customer address rows.
CREATE TABLE db_project_orders.order_addresses (
    order_address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    address_type VARCHAR(10) NOT NULL CHECK (address_type IN ('billing','shipping')),
    recipient_name VARCHAR(200) NOT NULL,
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255),
    city VARCHAR(120) NOT NULL,
    region VARCHAR(120),
    postal_code VARCHAR(30),
    country_code CHAR(2) NOT NULL,
    phone_snapshot VARCHAR(40),
    UNIQUE (order_id, address_type)
);

-- 6. Order status transition history.
CREATE TABLE db_project_orders.order_status_history (
    status_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    old_status VARCHAR(24),
    new_status VARCHAR(24) NOT NULL
        CHECK (new_status IN ('pending_payment','confirmed','processing','partially_shipped','shipped','delivered','cancelled','closed')),
    changed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    change_reason TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (old_status IS NULL OR old_status IN ('pending_payment','confirmed','processing','partially_shipped','shipped','delivered','cancelled','closed')),
    CHECK (old_status IS NULL OR old_status <> new_status)
);
CREATE INDEX idx_orders_status_history_order_time ON db_project_orders.order_status_history(order_id, changed_at DESC);

-- 7. An order line can be allocated across multiple warehouses.
CREATE TABLE db_project_orders.order_allocations (
    allocation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    order_item_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    allocated_quantity INTEGER NOT NULL CHECK (allocated_quantity > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'reserved'
        CHECK (status IN ('reserved','picking','fulfilled','released','cancelled')),
    allocated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id, order_item_id)
        REFERENCES db_project_orders.order_items(order_id, order_item_id) ON DELETE RESTRICT,
    UNIQUE (order_item_id, warehouse_id)
);
CREATE INDEX idx_orders_allocations_warehouse_status ON db_project_orders.order_allocations(warehouse_id, status);
CREATE INDEX idx_orders_allocations_order ON db_project_orders.order_allocations(order_id);

-- 8. Cancellation requests, including partial order-item cancellations.
CREATE TABLE db_project_orders.order_cancellations (
    cancellation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    order_item_id BIGINT,
    quantity INTEGER CHECK (quantity > 0),
    reason_code VARCHAR(40) NOT NULL,
    notes TEXT,
    status VARCHAR(16) NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested','approved','rejected','completed')),
    requested_by_customer_id BIGINT REFERENCES db_project_identity.customers(customer_id) ON DELETE SET NULL,
    reviewed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMPTZ,
    CHECK ((order_item_id IS NULL AND quantity IS NULL) OR (order_item_id IS NOT NULL AND quantity IS NOT NULL)),
    CHECK (reviewed_at IS NULL OR reviewed_at >= requested_at),
    FOREIGN KEY (order_id, order_item_id)
        REFERENCES db_project_orders.order_items(order_id, order_item_id) ON DELETE RESTRICT
);
CREATE INDEX idx_orders_cancellations_order ON db_project_orders.order_cancellations(order_id);

-- 9. Auditable manual price, tax, shipping or goodwill adjustments.
CREATE TABLE db_project_orders.order_adjustments (
    adjustment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    adjustment_type VARCHAR(24) NOT NULL
        CHECK (adjustment_type IN ('discount','shipping','tax','goodwill','correction')),
    amount NUMERIC(19,4) NOT NULL CHECK (amount <> 0),
    reason TEXT NOT NULL,
    created_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_orders_adjustments_order ON db_project_orders.order_adjustments(order_id);

-- 10. Idempotent checkout attempts; an order is created at most once per key.
CREATE TABLE db_project_orders.checkout_attempts (
    checkout_attempt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    cart_id BIGINT REFERENCES db_project_orders.shopping_carts(cart_id) ON DELETE SET NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    order_id BIGINT UNIQUE REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    status VARCHAR(16) NOT NULL DEFAULT 'started'
        CHECK (status IN ('started','succeeded','failed')),
    error_code VARCHAR(80),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    UNIQUE (customer_id, idempotency_key),
    CHECK ((status = 'succeeded' AND order_id IS NOT NULL AND completed_at IS NOT NULL)
        OR (status = 'started' AND completed_at IS NULL AND order_id IS NULL)
        OR (status = 'failed' AND completed_at IS NOT NULL AND order_id IS NULL)),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);
CREATE INDEX idx_orders_checkout_customer_time ON db_project_orders.checkout_attempts(customer_id, created_at DESC);

-- Complete the deferred cross-schema relationship from script 04.
ALTER TABLE db_project_inventory.inventory_reservations
    ADD CONSTRAINT fk_inventory_reservations_order
    FOREIGN KEY (order_id) REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT;

COMMIT;

-- Business rules to implement in transactional application code or stored procedures:
--  * Verify order header totals match the sum of order lines.
--  * Verify allocations do not exceed purchased quantities.
--  * Verify a cancellation belongs to the order and does not exceed remaining units.
--  * Keep inventory reservations and balance changes atomic under concurrent checkout.
--  * Verify cart/customer and checkout/customer consistency.
--  * Keep updated_at and status history synchronized with changes.
