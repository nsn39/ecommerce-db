-- 04_db_project_inventory.sql
-- PostgreSQL 15+ | E-commerce database project
-- Requires 02_db_project_catalog.sql and 03_db_project_warehouse.sql.
-- Run once on an empty inventory schema. All quantities are nonnegative integers.
-- Reservations use a nullable order_id until the orders schema is installed;
-- add its foreign key in the orders migration after that schema exists.
BEGIN;
CREATE SCHEMA IF NOT EXISTS db_project_inventory;

-- 1. Lot / batch metadata. Null lot_id denotes untracked stock.
CREATE TABLE db_project_inventory.inventory_lots (
    lot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    lot_code VARCHAR(100) NOT NULL,
    manufactured_on DATE,
    expires_on DATE,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (expires_on IS NULL OR manufactured_on IS NULL OR expires_on >= manufactured_on),
    UNIQUE (variant_id, lot_code),
    UNIQUE (lot_id, variant_id)
);

-- 2. Stock at a physical bin. Non-lot stock uses lot_key = 0.
-- available = quantity_on_hand - quantity_reserved; both updated atomically.
CREATE TABLE db_project_inventory.inventory_balances (
    balance_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL,
    bin_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    lot_id BIGINT,
    lot_key BIGINT GENERATED ALWAYS AS (COALESCE(lot_id, 0)) STORED,
    quantity_on_hand BIGINT NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    quantity_reserved BIGINT NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
    quantity_available BIGINT GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (quantity_reserved <= quantity_on_hand),
    FOREIGN KEY (warehouse_id, bin_id) REFERENCES db_project_warehouse.storage_bins(warehouse_id, bin_id) ON DELETE RESTRICT,
    FOREIGN KEY (lot_id, variant_id) REFERENCES db_project_inventory.inventory_lots(lot_id, variant_id) ON DELETE RESTRICT,
    UNIQUE (bin_id, variant_id, lot_key),
    UNIQUE (balance_id, variant_id)
);
CREATE INDEX idx_inventory_balances_warehouse_variant ON db_project_inventory.inventory_balances(warehouse_id, variant_id);
CREATE INDEX idx_inventory_balances_available ON db_project_inventory.inventory_balances(variant_id, warehouse_id) WHERE quantity_on_hand > quantity_reserved;

-- 3. Reservation rows are a ledger of claims on inventory.
-- The application must atomically increase/decrease balance.quantity_reserved
-- in the same transaction as inserting/updating a reservation.
CREATE TABLE db_project_inventory.inventory_reservations (
    reservation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    balance_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_balances(balance_id) ON DELETE RESTRICT,
    order_id BIGINT, -- linked to db_project_orders.orders in a later migration
    reservation_reference VARCHAR(80) NOT NULL UNIQUE,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active','consumed','released','expired')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    CHECK (expires_at IS NULL OR expires_at > reserved_at),
    CHECK ((status = 'active' AND resolved_at IS NULL) OR (status <> 'active' AND resolved_at IS NOT NULL))
);
CREATE INDEX idx_inventory_reservations_balance_status ON db_project_inventory.inventory_reservations(balance_id, status);
CREATE INDEX idx_inventory_reservations_order ON db_project_inventory.inventory_reservations(order_id) WHERE order_id IS NOT NULL;
CREATE INDEX idx_inventory_reservations_expiry ON db_project_inventory.inventory_reservations(expires_at) WHERE status = 'active';

-- 4. Append-only stock movement history (immutability enforced by application permissions).
CREATE TABLE db_project_inventory.inventory_movements (
    movement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    balance_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_balances(balance_id) ON DELETE RESTRICT,
    movement_type VARCHAR(24) NOT NULL CHECK (movement_type IN ('receipt','sale','return','adjustment','transfer_in','transfer_out','damage','relocation_in','relocation_out')),
    quantity_delta BIGINT NOT NULL CHECK (quantity_delta <> 0),
    reference_type VARCHAR(40),
    reference_id BIGINT,
    reason TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    recorded_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL
);
CREATE INDEX idx_inventory_movements_balance_time ON db_project_inventory.inventory_movements(balance_id, occurred_at DESC);
CREATE INDEX idx_inventory_movements_time ON db_project_inventory.inventory_movements(occurred_at);

-- 5. Explicit adjustments, supported by movement records.
CREATE TABLE db_project_inventory.inventory_adjustments (
    adjustment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    balance_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_balances(balance_id) ON DELETE RESTRICT,
    movement_id BIGINT UNIQUE REFERENCES db_project_inventory.inventory_movements(movement_id) ON DELETE RESTRICT,
    quantity_delta BIGINT NOT NULL CHECK (quantity_delta <> 0),
    reason_code VARCHAR(32) NOT NULL CHECK (reason_code IN ('damage','loss','found','count_correction','other')),
    notes TEXT,
    adjusted_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    adjusted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_inventory_adjustments_balance ON db_project_inventory.inventory_adjustments(balance_id);

-- 6. Physical counting sessions.
CREATE TABLE db_project_inventory.stock_counts (
    stock_count_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    count_reference VARCHAR(80) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'planned' CHECK (status IN ('planned','in_progress','completed','cancelled')),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (completed_at IS NULL OR (started_at IS NOT NULL AND completed_at >= started_at)),
    UNIQUE (stock_count_id, warehouse_id)
);
CREATE INDEX idx_stock_counts_warehouse_status ON db_project_inventory.stock_counts(warehouse_id, status);

-- 7. Individual count results, including differences from the expected balance.
CREATE TABLE db_project_inventory.stock_count_items (
    stock_count_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stock_count_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    balance_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_balances(balance_id) ON DELETE RESTRICT,
    expected_quantity BIGINT NOT NULL CHECK (expected_quantity >= 0),
    counted_quantity BIGINT CHECK (counted_quantity >= 0),
    variance BIGINT GENERATED ALWAYS AS (counted_quantity - expected_quantity) STORED,
    counted_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    counted_at TIMESTAMPTZ,
    FOREIGN KEY (stock_count_id, warehouse_id) REFERENCES db_project_inventory.stock_counts(stock_count_id, warehouse_id) ON DELETE RESTRICT,
    UNIQUE (stock_count_id, balance_id)
);
CREATE INDEX idx_stock_count_items_balance ON db_project_inventory.stock_count_items(balance_id);

-- 8. Inter-warehouse transfer headers.
CREATE TABLE db_project_inventory.inventory_transfers (
    transfer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_reference VARCHAR(80) NOT NULL UNIQUE,
    source_warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    destination_warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    status VARCHAR(24) NOT NULL DEFAULT 'requested' CHECK (status IN ('requested','approved','in_transit','partially_received','received','cancelled')),
    requested_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    CHECK (source_warehouse_id <> destination_warehouse_id),
    CHECK (received_at IS NULL OR shipped_at IS NOT NULL),
    CHECK (received_at IS NULL OR received_at >= shipped_at)
);
CREATE INDEX idx_inventory_transfers_source_status ON db_project_inventory.inventory_transfers(source_warehouse_id, status);
CREATE INDEX idx_inventory_transfers_destination_status ON db_project_inventory.inventory_transfers(destination_warehouse_id, status);

-- 9. Quantities requested, shipped and received per SKU.
CREATE TABLE db_project_inventory.inventory_transfer_items (
    transfer_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_transfers(transfer_id) ON DELETE RESTRICT,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    quantity_requested BIGINT NOT NULL CHECK (quantity_requested > 0),
    quantity_shipped BIGINT NOT NULL DEFAULT 0 CHECK (quantity_shipped >= 0),
    quantity_received BIGINT NOT NULL DEFAULT 0 CHECK (quantity_received >= 0),
    CHECK (quantity_shipped <= quantity_requested),
    CHECK (quantity_received <= quantity_shipped),
    UNIQUE (transfer_id, variant_id)
);
CREATE INDEX idx_inventory_transfer_items_variant ON db_project_inventory.inventory_transfer_items(variant_id);

-- 10. Transfer event timeline.
CREATE TABLE db_project_inventory.inventory_transfer_events (
    transfer_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id BIGINT NOT NULL REFERENCES db_project_inventory.inventory_transfers(transfer_id) ON DELETE RESTRICT,
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('requested','approved','picked','shipped','partially_received','received','cancelled','note')),
    notes TEXT,
    performed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_inventory_transfer_events_timeline ON db_project_inventory.inventory_transfer_events(transfer_id, occurred_at);

-- 11. Replenishment policy per warehouse and SKU.
CREATE TABLE db_project_inventory.inventory_reorder_rules (
    reorder_rule_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock_level BIGINT NOT NULL CHECK (target_stock_level > reorder_point),
    minimum_order_quantity BIGINT NOT NULL DEFAULT 1 CHECK (minimum_order_quantity > 0),
    lead_time_days INTEGER CHECK (lead_time_days >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (warehouse_id, variant_id)
);
CREATE INDEX idx_inventory_reorder_rules_variant ON db_project_inventory.inventory_reorder_rules(variant_id);

-- 12. Daily warehouse/SKU availability snapshots for operational reporting.
CREATE TABLE db_project_inventory.inventory_availability_snapshots (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    quantity_on_hand BIGINT NOT NULL CHECK (quantity_on_hand >= 0),
    quantity_reserved BIGINT NOT NULL CHECK (quantity_reserved >= 0),
    quantity_available BIGINT GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (quantity_reserved <= quantity_on_hand),
    UNIQUE (snapshot_date, warehouse_id, variant_id)
);
CREATE INDEX idx_inventory_snapshots_variant_date ON db_project_inventory.inventory_availability_snapshots(variant_id, snapshot_date DESC);

COMMIT;
