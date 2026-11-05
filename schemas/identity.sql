-- 01_identity.sql
-- PostgreSQL 15+ | E-commerce database project
-- Identity domain. Run before scripts that reference identity tables.
BEGIN;

CREATE SCHEMA IF NOT EXISTS db_project_identity;

CREATE TABLE db_project_identity.customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_number VARCHAR(32) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'suspended', 'closed')),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE db_project_identity.customer_profiles (
    customer_id BIGINT PRIMARY KEY
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    preferred_language VARCHAR(16),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE db_project_identity.customer_addresses (
    address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    label VARCHAR(50),
    recipient_name VARCHAR(200) NOT NULL,
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255),
    city VARCHAR(120) NOT NULL,
    region VARCHAR(120),
    postal_code VARCHAR(30),
    country_code CHAR(2) NOT NULL,
    is_default_shipping BOOLEAN NOT NULL DEFAULT FALSE,
    is_default_billing BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_addresses_customer
    ON db_project_identity.customer_addresses(customer_id);
CREATE UNIQUE INDEX uq_customer_default_shipping
    ON db_project_identity.customer_addresses(customer_id)
    WHERE is_default_shipping;
CREATE UNIQUE INDEX uq_customer_default_billing
    ON db_project_identity.customer_addresses(customer_id)
    WHERE is_default_billing;

CREATE TABLE db_project_identity.customer_contacts (
    contact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    contact_type VARCHAR(10) NOT NULL
        CHECK (contact_type IN ('email', 'phone')),
    contact_value VARCHAR(320) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (customer_id, contact_type, contact_value)
);
CREATE INDEX idx_customer_contacts_customer
    ON db_project_identity.customer_contacts(customer_id);
CREATE UNIQUE INDEX uq_customer_primary_contact_type
    ON db_project_identity.customer_contacts(customer_id, contact_type)
    WHERE is_primary;
CREATE UNIQUE INDEX uq_customer_email_ci
    ON db_project_identity.customer_contacts(lower(contact_value))
    WHERE contact_type = 'email';

CREATE TABLE db_project_identity.customer_preferences (
    customer_id BIGINT PRIMARY KEY
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    preferred_currency CHAR(3) NOT NULL DEFAULT 'USD',
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    email_notifications BOOLEAN NOT NULL DEFAULT TRUE,
    sms_notifications BOOLEAN NOT NULL DEFAULT FALSE,
    marketing_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE db_project_identity.customer_auth_credentials (
    customer_id BIGINT PRIMARY KEY
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    password_hash TEXT,
    external_identity_provider VARCHAR(80),
    external_subject VARCHAR(255),
    password_changed_at TIMESTAMPTZ,
    failed_login_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (failed_login_attempts >= 0),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        password_hash IS NOT NULL OR
        (external_identity_provider IS NOT NULL AND external_subject IS NOT NULL)
    ),
    CHECK (
        (external_identity_provider IS NULL AND external_subject IS NULL)
        OR (external_identity_provider IS NOT NULL AND external_subject IS NOT NULL)
    ),
    UNIQUE (external_identity_provider, external_subject)
);

CREATE TABLE db_project_identity.customer_sessions (
    session_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL
        REFERENCES db_project_identity.customers(customer_id) ON DELETE CASCADE,
    session_token_hash CHAR(64) NOT NULL UNIQUE,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CHECK (expires_at > created_at),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);
CREATE INDEX idx_customer_sessions_customer_expires
    ON db_project_identity.customer_sessions(customer_id, expires_at);

CREATE TABLE db_project_identity.employees (
    employee_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_number VARCHAR(32) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    work_email VARCHAR(320) NOT NULL,
    employment_status VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (employment_status IN ('active', 'on_leave', 'terminated')),
    hired_at DATE NOT NULL,
    terminated_at DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (terminated_at IS NULL OR terminated_at >= hired_at)
);
CREATE UNIQUE INDEX uq_employees_work_email_ci
    ON db_project_identity.employees(lower(work_email));

CREATE TABLE db_project_identity.roles (
    role_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    role_code VARCHAR(60) NOT NULL UNIQUE,
    role_name VARCHAR(120) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE db_project_identity.employee_roles (
    employee_id BIGINT NOT NULL
        REFERENCES db_project_identity.employees(employee_id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL
        REFERENCES db_project_identity.roles(role_id) ON DELETE RESTRICT,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assigned_by_employee_id BIGINT
        REFERENCES db_project_identity.employees(employee_id) ON DELETE SET NULL,
    PRIMARY KEY (employee_id, role_id)
);
CREATE INDEX idx_employee_roles_role
    ON db_project_identity.employee_roles(role_id);

COMMIT;
