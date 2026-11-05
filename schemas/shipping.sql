-- 07_db_project_shipping.sql
-- PostgreSQL 15+. Requires identity, catalog, warehouse, inventory, orders schemas.
-- Shipping and fulfillment domain. Create after 05_db_project_orders.sql.
BEGIN;
CREATE SCHEMA IF NOT EXISTS db_project_shipping;

-- 1. One warehouse-specific fulfillment request for an order.
CREATE TABLE db_project_shipping.fulfillment_orders (
    fulfillment_order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fulfillment_number VARCHAR(40) NOT NULL UNIQUE,
    order_id BIGINT NOT NULL REFERENCES db_project_orders.orders(order_id) ON DELETE RESTRICT,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    status VARCHAR(24) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','allocated','picking','packing','ready_to_ship','shipped','cancelled')),
    priority SMALLINT NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    UNIQUE (fulfillment_order_id, order_id),
    UNIQUE (fulfillment_order_id, warehouse_id),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);
CREATE INDEX idx_shipping_fulfillment_order ON db_project_shipping.fulfillment_orders(order_id);
CREATE INDEX idx_shipping_fulfillment_queue ON db_project_shipping.fulfillment_orders(warehouse_id, status, priority);

-- 2. Items allocated to each fulfillment request.
CREATE TABLE db_project_shipping.fulfillment_items (
    fulfillment_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fulfillment_order_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    order_item_id BIGINT NOT NULL,
    allocation_id BIGINT NOT NULL UNIQUE REFERENCES db_project_orders.order_allocations(allocation_id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    picked_quantity INTEGER NOT NULL DEFAULT 0 CHECK (picked_quantity >= 0),
    packed_quantity INTEGER NOT NULL DEFAULT 0 CHECK (packed_quantity >= 0),
    FOREIGN KEY (fulfillment_order_id, order_id)
        REFERENCES db_project_shipping.fulfillment_orders(fulfillment_order_id, order_id) ON DELETE RESTRICT,
    FOREIGN KEY (order_id, order_item_id)
        REFERENCES db_project_orders.order_items(order_id, order_item_id) ON DELETE RESTRICT,
    CHECK (picked_quantity <= quantity),
    CHECK (packed_quantity <= picked_quantity),
    UNIQUE (fulfillment_order_id, fulfillment_item_id)
);
CREATE INDEX idx_shipping_fulfillment_items_order_item ON db_project_shipping.fulfillment_items(order_item_id);

-- 3. Picking work assigned to warehouse employees.
CREATE TABLE db_project_shipping.pick_tasks (
    pick_task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fulfillment_order_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    assigned_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','assigned','in_progress','completed','cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (fulfillment_order_id, warehouse_id)
        REFERENCES db_project_shipping.fulfillment_orders(fulfillment_order_id, warehouse_id) ON DELETE RESTRICT,
    UNIQUE (pick_task_id, fulfillment_order_id),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (completed_at IS NULL OR completed_at >= COALESCE(started_at, created_at))
);
CREATE INDEX idx_shipping_pick_tasks_queue ON db_project_shipping.pick_tasks(warehouse_id, status);
CREATE INDEX idx_shipping_pick_tasks_employee ON db_project_shipping.pick_tasks(assigned_employee_id, status);

-- 4. Specific bin-level items to pick.
CREATE TABLE db_project_shipping.pick_task_items (
    pick_task_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pick_task_id BIGINT NOT NULL,
    fulfillment_order_id BIGINT NOT NULL,
    fulfillment_item_id BIGINT NOT NULL,
    inventory_balance_id BIGINT REFERENCES db_project_inventory.inventory_balances(balance_id) ON DELETE RESTRICT,
    requested_quantity INTEGER NOT NULL CHECK (requested_quantity > 0),
    picked_quantity INTEGER NOT NULL DEFAULT 0 CHECK (picked_quantity >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','picked','short','cancelled')),
    FOREIGN KEY (pick_task_id, fulfillment_order_id)
        REFERENCES db_project_shipping.pick_tasks(pick_task_id, fulfillment_order_id) ON DELETE RESTRICT,
    FOREIGN KEY (fulfillment_order_id, fulfillment_item_id)
        REFERENCES db_project_shipping.fulfillment_items(fulfillment_order_id, fulfillment_item_id) ON DELETE RESTRICT,
    CHECK (picked_quantity <= requested_quantity)
);
CREATE INDEX idx_shipping_pick_items_task ON db_project_shipping.pick_task_items(pick_task_id);

-- 5. Carrier directory.
CREATE TABLE db_project_shipping.shipping_carriers (
    carrier_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    carrier_code VARCHAR(40) NOT NULL UNIQUE,
    carrier_name VARCHAR(160) NOT NULL,
    tracking_url_template TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 6. Services offered by a carrier (ground, express, etc.).
CREATE TABLE db_project_shipping.shipping_services (
    shipping_service_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    carrier_id BIGINT NOT NULL REFERENCES db_project_shipping.shipping_carriers(carrier_id) ON DELETE RESTRICT,
    service_code VARCHAR(50) NOT NULL,
    service_name VARCHAR(150) NOT NULL,
    estimated_transit_days_min INTEGER CHECK (estimated_transit_days_min >= 0),
    estimated_transit_days_max INTEGER CHECK (estimated_transit_days_max >= 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (carrier_id, service_code),
    UNIQUE (carrier_id, shipping_service_id),
    CHECK (estimated_transit_days_min IS NULL OR estimated_transit_days_max IS NULL
           OR estimated_transit_days_max >= estimated_transit_days_min)
);

-- 7. Shipment records; one fulfillment order can create multiple shipments.
CREATE TABLE db_project_shipping.shipments (
    shipment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    fulfillment_order_id BIGINT NOT NULL REFERENCES db_project_shipping.fulfillment_orders(fulfillment_order_id) ON DELETE RESTRICT,
    carrier_id BIGINT NOT NULL REFERENCES db_project_shipping.shipping_carriers(carrier_id) ON DELETE RESTRICT,
    shipping_service_id BIGINT NOT NULL,
    tracking_number VARCHAR(120),
    status VARCHAR(24) NOT NULL DEFAULT 'created'
        CHECK (status IN ('created','label_created','in_transit','out_for_delivery','delivered','delivery_failed','lost','cancelled')),
    shipping_cost NUMERIC(19,4) CHECK (shipping_cost >= 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMPTZ,
    estimated_delivery_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    FOREIGN KEY (carrier_id, shipping_service_id)
        REFERENCES db_project_shipping.shipping_services(carrier_id, shipping_service_id) ON DELETE RESTRICT,
    UNIQUE (carrier_id, tracking_number),
    UNIQUE (shipment_id, fulfillment_order_id),
    CHECK (shipped_at IS NULL OR shipped_at >= created_at),
    CHECK (delivered_at IS NULL OR delivered_at >= COALESCE(shipped_at, created_at))
);
CREATE INDEX idx_shipping_shipments_fulfillment ON db_project_shipping.shipments(fulfillment_order_id);
CREATE INDEX idx_shipping_shipments_status ON db_project_shipping.shipments(status, created_at DESC);

-- 8. Physical packed boxes or parcels, including those awaiting shipment.
CREATE TABLE db_project_shipping.packages (
    package_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    package_number VARCHAR(60) NOT NULL UNIQUE,
    fulfillment_order_id BIGINT NOT NULL REFERENCES db_project_shipping.fulfillment_orders(fulfillment_order_id) ON DELETE RESTRICT,
    package_type VARCHAR(30) NOT NULL DEFAULT 'box'
        CHECK (package_type IN ('box','envelope','pallet','other')),
    weight_kg NUMERIC(12,3) CHECK (weight_kg > 0),
    length_cm NUMERIC(12,2) CHECK (length_cm > 0),
    width_cm NUMERIC(12,2) CHECK (width_cm > 0),
    height_cm NUMERIC(12,2) CHECK (height_cm > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'packing'
        CHECK (status IN ('packing','packed','shipped','voided')),
    packed_by_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    packed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (package_id, fulfillment_order_id)
);
CREATE INDEX idx_shipping_packages_fulfillment ON db_project_shipping.packages(fulfillment_order_id);

-- 9. Contents of each package. A fulfillment item can be split across packages.
CREATE TABLE db_project_shipping.package_items (
    package_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    package_id BIGINT NOT NULL,
    fulfillment_order_id BIGINT NOT NULL,
    fulfillment_item_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    FOREIGN KEY (package_id, fulfillment_order_id)
        REFERENCES db_project_shipping.packages(package_id, fulfillment_order_id) ON DELETE RESTRICT,
    FOREIGN KEY (fulfillment_order_id, fulfillment_item_id)
        REFERENCES db_project_shipping.fulfillment_items(fulfillment_order_id, fulfillment_item_id) ON DELETE RESTRICT,
    UNIQUE (package_id, fulfillment_item_id)
);
CREATE INDEX idx_shipping_package_items_fulfillment_item ON db_project_shipping.package_items(fulfillment_item_id);

-- 10. Link packages to shipments; a package may be assigned to one shipment.
CREATE TABLE db_project_shipping.shipment_packages (
    shipment_id BIGINT NOT NULL,
    package_id BIGINT NOT NULL UNIQUE,
    fulfillment_order_id BIGINT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shipment_id, package_id),
    FOREIGN KEY (shipment_id, fulfillment_order_id)
        REFERENCES db_project_shipping.shipments(shipment_id, fulfillment_order_id) ON DELETE RESTRICT,
    FOREIGN KEY (package_id, fulfillment_order_id)
        REFERENCES db_project_shipping.packages(package_id, fulfillment_order_id) ON DELETE RESTRICT
);

-- 11. Append-only carrier tracking events (enforce append-only in application/permissions).
CREATE TABLE db_project_shipping.tracking_events (
    tracking_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES db_project_shipping.shipments(shipment_id) ON DELETE RESTRICT,
    provider_event_id VARCHAR(120),
    event_code VARCHAR(60) NOT NULL,
    event_description TEXT,
    event_location VARCHAR(255),
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_event JSONB,
    UNIQUE (shipment_id, provider_event_id)
);
CREATE INDEX idx_shipping_tracking_events_shipment_time
    ON db_project_shipping.tracking_events(shipment_id, occurred_at DESC);

-- 12. Delivery attempts, including failures and final successful delivery.
CREATE TABLE db_project_shipping.delivery_attempts (
    delivery_attempt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES db_project_shipping.shipments(shipment_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    attempted_at TIMESTAMPTZ NOT NULL,
    outcome VARCHAR(24) NOT NULL
        CHECK (outcome IN ('delivered','recipient_unavailable','address_issue','access_issue','refused','other_failure')),
    notes TEXT,
    next_attempt_at TIMESTAMPTZ,
    proof_of_delivery_reference TEXT,
    UNIQUE (shipment_id, attempt_number),
    CHECK (next_attempt_at IS NULL OR next_attempt_at > attempted_at)
);
CREATE INDEX idx_shipping_delivery_attempts_shipment ON db_project_shipping.delivery_attempts(shipment_id, attempted_at DESC);

COMMIT;
