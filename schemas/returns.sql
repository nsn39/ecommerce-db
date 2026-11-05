-- 08_db_project_returns.sql
-- PostgreSQL 15+ | E-commerce returns and reverse logistics
-- Prerequisites: scripts 01-07 (identity, catalog, warehouse, inventory,
-- orders, payments, shipping). Creates 8 tables.
BEGIN;

CREATE SCHEMA IF NOT EXISTS db_project_returns;

CREATE TABLE db_project_returns.return_reasons (
    reason_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason_code VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255) NOT NULL,
    requires_inspection BOOLEAN NOT NULL DEFAULT TRUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE db_project_returns.return_requests (
    return_request_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_number VARCHAR(40) NOT NULL UNIQUE,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    status VARCHAR(25) NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested','under_review','approved','rejected',
                         'awaiting_shipment','in_transit','received','inspecting',
                         'completed','cancelled')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    customer_notes TEXT,
    internal_notes TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (resolved_at IS NULL OR resolved_at >= requested_at),
    UNIQUE (return_request_id, order_id)
);
CREATE INDEX idx_return_requests_customer ON db_project_returns.return_requests(customer_id, requested_at DESC);
CREATE INDEX idx_return_requests_order ON db_project_returns.return_requests(order_id);
CREATE INDEX idx_return_requests_status ON db_project_returns.return_requests(status, requested_at);

CREATE TABLE db_project_returns.return_items (
    return_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_request_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    order_item_id BIGINT NOT NULL,
    reason_id BIGINT NOT NULL REFERENCES db_project_returns.return_reasons(reason_id) ON DELETE RESTRICT,
    requested_quantity INTEGER NOT NULL CHECK (requested_quantity > 0),
    received_quantity INTEGER NOT NULL DEFAULT 0 CHECK (received_quantity >= 0),
    item_condition VARCHAR(25)
        CHECK (item_condition IN ('unopened','like_new','used','damaged','defective','unknown')),
    customer_comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (received_quantity <= requested_quantity),
    FOREIGN KEY (return_request_id, order_id)
        REFERENCES db_project_returns.return_requests(return_request_id, order_id) ON DELETE RESTRICT,
    FOREIGN KEY (order_id, order_item_id)
        REFERENCES db_project_orders.order_items(order_id, order_item_id) ON DELETE RESTRICT,
    UNIQUE (return_request_id, order_item_id),
    UNIQUE (return_request_id, return_item_id)
);
CREATE INDEX idx_return_items_order_item ON db_project_returns.return_items(order_item_id);
CREATE INDEX idx_return_items_reason ON db_project_returns.return_items(reason_id);

CREATE TABLE db_project_returns.return_authorizations (
    authorization_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_request_id BIGINT NOT NULL UNIQUE
        REFERENCES db_project_returns.return_requests(return_request_id) ON DELETE RESTRICT,
    authorization_code VARCHAR(60) NOT NULL UNIQUE,
    decision VARCHAR(15) NOT NULL CHECK (decision IN ('approved','rejected')),
    decided_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    decision_notes TEXT,
    CHECK (expires_at IS NULL OR expires_at > decided_at)
);

CREATE TABLE db_project_returns.return_shipments (
    return_shipment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_request_id BIGINT NOT NULL REFERENCES db_project_returns.return_requests(return_request_id) ON DELETE RESTRICT,
    destination_warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    carrier_id BIGINT REFERENCES db_project_shipping.shipping_carriers(carrier_id) ON DELETE RESTRICT,
    tracking_number VARCHAR(120),
    status VARCHAR(20) NOT NULL DEFAULT 'label_created'
        CHECK (status IN ('label_created','in_transit','delivered','lost','cancelled')),
    shipped_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (received_at IS NULL OR shipped_at IS NULL OR received_at >= shipped_at)
);
CREATE INDEX idx_return_shipments_request ON db_project_returns.return_shipments(return_request_id);
CREATE INDEX idx_return_shipments_tracking ON db_project_returns.return_shipments(carrier_id, tracking_number)
    WHERE tracking_number IS NOT NULL;

CREATE TABLE db_project_returns.return_inspections (
    inspection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_request_id BIGINT NOT NULL,
    return_item_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    inspected_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    inspected_quantity INTEGER NOT NULL CHECK (inspected_quantity > 0),
    condition_grade VARCHAR(20) NOT NULL
        CHECK (condition_grade IN ('new','like_new','used','damaged','defective')),
    result VARCHAR(20) NOT NULL
        CHECK (result IN ('accepted','partially_accepted','rejected')),
    inspection_notes TEXT,
    inspected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (return_request_id, return_item_id)
        REFERENCES db_project_returns.return_items(return_request_id, return_item_id) ON DELETE RESTRICT
);
CREATE INDEX idx_return_inspections_item ON db_project_returns.return_inspections(return_item_id);
CREATE INDEX idx_return_inspections_warehouse ON db_project_returns.return_inspections(warehouse_id, inspected_at);

CREATE TABLE db_project_returns.return_dispositions (
    disposition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    inspection_id BIGINT NOT NULL REFERENCES db_project_returns.return_inspections(inspection_id) ON DELETE RESTRICT,
    disposition_type VARCHAR(20) NOT NULL
        CHECK (disposition_type IN ('restock','quarantine','refurbish','return_to_supplier','dispose')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    destination_warehouse_id BIGINT REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    destination_bin_id BIGINT REFERENCES db_project_warehouse.storage_bins(bin_id) ON DELETE RESTRICT,
    processed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    processed_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (destination_bin_id IS NULL OR destination_warehouse_id IS NOT NULL)
);
CREATE INDEX idx_return_dispositions_inspection ON db_project_returns.return_dispositions(inspection_id);

CREATE TABLE db_project_returns.return_status_history (
    status_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_request_id BIGINT NOT NULL REFERENCES db_project_returns.return_requests(return_request_id) ON DELETE RESTRICT,
    old_status VARCHAR(25),
    new_status VARCHAR(25) NOT NULL
        CHECK (new_status IN ('requested','under_review','approved','rejected',
                             'awaiting_shipment','in_transit','received','inspecting',
                             'completed','cancelled')),
    changed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    change_reason TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_return_status_history_request ON db_project_returns.return_status_history(return_request_id, changed_at DESC);

COMMIT;
