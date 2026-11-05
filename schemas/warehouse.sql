-- 03_db_project_warehouse.sql
-- PostgreSQL 15+ | E-commerce database project
-- Physical warehouse locations, employees, equipment, and operational tasks.
-- Dependency: 01_db_project_identity.sql (employees).
-- This script creates an empty schema; rerunning it after successful creation will fail.
BEGIN;

CREATE SCHEMA IF NOT EXISTS db_project_warehouse;

-- 1. Physical facilities
CREATE TABLE db_project_warehouse.warehouses (
    warehouse_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_code VARCHAR(32) NOT NULL UNIQUE,
    warehouse_name VARCHAR(160) NOT NULL,
    warehouse_type VARCHAR(24) NOT NULL DEFAULT 'fulfillment'
        CHECK (warehouse_type IN ('fulfillment', 'distribution', 'returns', 'mixed')),
    status VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (status IN ('planned', 'active', 'inactive', 'closed')),
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255),
    city VARCHAR(120) NOT NULL,
    region VARCHAR(120),
    postal_code VARCHAR(30),
    country_code CHAR(2) NOT NULL,
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    floor_area_sq_m NUMERIC(14,2) CHECK (floor_area_sq_m > 0),
    opened_on DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_warehouses_status ON db_project_warehouse.warehouses(status);
CREATE INDEX idx_warehouses_country_region ON db_project_warehouse.warehouses(country_code, region);

-- 2. Warehouse zones: receiving, storage, picking, packing, etc.
CREATE TABLE db_project_warehouse.warehouse_zones (
    zone_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    zone_code VARCHAR(32) NOT NULL,
    zone_name VARCHAR(120) NOT NULL,
    zone_type VARCHAR(24) NOT NULL
        CHECK (zone_type IN ('receiving', 'storage', 'picking', 'packing', 'shipping', 'returns', 'quarantine', 'other')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (warehouse_id, zone_code),
    UNIQUE (warehouse_id, zone_id)
);
CREATE INDEX idx_warehouse_zones_warehouse ON db_project_warehouse.warehouse_zones(warehouse_id);

-- 3. Aisles within a zone
CREATE TABLE db_project_warehouse.storage_aisles (
    aisle_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL,
    zone_id BIGINT NOT NULL,
    aisle_code VARCHAR(32) NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (warehouse_id, zone_id)
        REFERENCES db_project_warehouse.warehouse_zones(warehouse_id, zone_id) ON DELETE RESTRICT,
    UNIQUE (zone_id, aisle_code),
    UNIQUE (warehouse_id, zone_id, aisle_id)
);
CREATE INDEX idx_storage_aisles_zone ON db_project_warehouse.storage_aisles(zone_id);

-- 4. Racks within an aisle
CREATE TABLE db_project_warehouse.storage_racks (
    rack_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL,
    zone_id BIGINT NOT NULL,
    aisle_id BIGINT NOT NULL,
    rack_code VARCHAR(32) NOT NULL,
    max_load_kg NUMERIC(12,2) CHECK (max_load_kg > 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (warehouse_id, zone_id, aisle_id)
        REFERENCES db_project_warehouse.storage_aisles(warehouse_id, zone_id, aisle_id) ON DELETE RESTRICT,
    UNIQUE (aisle_id, rack_code),
    UNIQUE (warehouse_id, zone_id, aisle_id, rack_id)
);
CREATE INDEX idx_storage_racks_aisle ON db_project_warehouse.storage_racks(aisle_id);

-- 5. Addressable storage bins; future inventory balances can reference bin_id
CREATE TABLE db_project_warehouse.storage_bins (
    bin_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL,
    zone_id BIGINT NOT NULL,
    aisle_id BIGINT NOT NULL,
    rack_id BIGINT NOT NULL,
    bin_code VARCHAR(40) NOT NULL,
    level_number INTEGER NOT NULL DEFAULT 1 CHECK (level_number > 0),
    bin_type VARCHAR(24) NOT NULL DEFAULT 'standard'
        CHECK (bin_type IN ('standard', 'bulk', 'pallet', 'cold', 'hazmat', 'quarantine')),
    capacity_units INTEGER CHECK (capacity_units > 0),
    max_load_kg NUMERIC(12,2) CHECK (max_load_kg > 0),
    is_pickable BOOLEAN NOT NULL DEFAULT TRUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (warehouse_id, zone_id, aisle_id, rack_id)
        REFERENCES db_project_warehouse.storage_racks(warehouse_id, zone_id, aisle_id, rack_id) ON DELETE RESTRICT,
    UNIQUE (rack_id, bin_code),
    UNIQUE (warehouse_id, bin_id)
);
CREATE INDEX idx_storage_bins_rack ON db_project_warehouse.storage_bins(rack_id);
CREATE INDEX idx_storage_bins_warehouse_pickable
    ON db_project_warehouse.storage_bins(warehouse_id, is_pickable) WHERE is_active;

-- 6. Employee assignments to warehouse facilities
CREATE TABLE db_project_warehouse.warehouse_employees (
    warehouse_employee_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    employee_id BIGINT NOT NULL REFERENCES db_project_identity.employees(employee_id) ON DELETE RESTRICT,
    assignment_role VARCHAR(60) NOT NULL,
    assigned_from DATE NOT NULL DEFAULT CURRENT_DATE,
    assigned_until DATE,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK (assigned_until IS NULL OR assigned_until >= assigned_from),
    UNIQUE (warehouse_id, employee_id, assigned_from)
);
CREATE INDEX idx_warehouse_employees_employee ON db_project_warehouse.warehouse_employees(employee_id);
CREATE UNIQUE INDEX uq_warehouse_employees_primary_active
    ON db_project_warehouse.warehouse_employees(employee_id)
    WHERE is_primary AND assigned_until IS NULL;

-- 7. Equipment assigned to facilities
CREATE TABLE db_project_warehouse.warehouse_equipment (
    equipment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    equipment_code VARCHAR(50) NOT NULL UNIQUE,
    equipment_type VARCHAR(32) NOT NULL
        CHECK (equipment_type IN ('forklift', 'pallet_jack', 'scanner', 'conveyor', 'robot', 'printer', 'other')),
    manufacturer VARCHAR(120),
    model VARCHAR(120),
    serial_number VARCHAR(120) UNIQUE,
    status VARCHAR(24) NOT NULL DEFAULT 'available'
        CHECK (status IN ('available', 'in_use', 'maintenance', 'retired')),
    purchased_on DATE,
    last_serviced_on DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_warehouse_equipment_warehouse_status
    ON db_project_warehouse.warehouse_equipment(warehouse_id, status);

-- 8. Warehouse operational tasks (independent of the later orders/shipping schemas)
CREATE TABLE db_project_warehouse.warehouse_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouses(warehouse_id) ON DELETE RESTRICT,
    task_code VARCHAR(50) NOT NULL UNIQUE,
    task_type VARCHAR(24) NOT NULL
        CHECK (task_type IN ('receiving', 'putaway', 'replenishment', 'picking', 'packing', 'counting', 'transfer', 'maintenance', 'other')),
    status VARCHAR(24) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'assigned', 'in_progress', 'completed', 'cancelled', 'failed')),
    priority SMALLINT NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
    assigned_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    source_bin_id BIGINT,
    destination_bin_id BIGINT,
    description TEXT,
    due_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (warehouse_id, source_bin_id)
        REFERENCES db_project_warehouse.storage_bins(warehouse_id, bin_id) ON DELETE RESTRICT,
    FOREIGN KEY (warehouse_id, destination_bin_id)
        REFERENCES db_project_warehouse.storage_bins(warehouse_id, bin_id) ON DELETE RESTRICT,
    CHECK (source_bin_id IS NULL OR destination_bin_id IS NULL OR source_bin_id <> destination_bin_id),
    CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
);
CREATE INDEX idx_warehouse_tasks_queue
    ON db_project_warehouse.warehouse_tasks(warehouse_id, priority, created_at)
    WHERE status IN ('pending', 'assigned');
CREATE INDEX idx_warehouse_tasks_assignee
    ON db_project_warehouse.warehouse_tasks(assigned_employee_id, status);

-- 9. Append-only event history for warehouse tasks (enforced by application permissions)
CREATE TABLE db_project_warehouse.warehouse_task_events (
    task_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES db_project_warehouse.warehouse_tasks(task_id) ON DELETE RESTRICT,
    event_type VARCHAR(40) NOT NULL,
    previous_status VARCHAR(24),
    new_status VARCHAR(24),
    actor_employee_id BIGINT REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    notes TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (event_type <> '')
);
CREATE INDEX idx_warehouse_task_events_task_time
    ON db_project_warehouse.warehouse_task_events(task_id, occurred_at);

COMMIT;
