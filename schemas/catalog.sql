-- 02_db_project_catalog.sql
-- PostgreSQL 15+ | E-commerce database project
-- Catalog domain: product master data, variants/SKUs, categories, attributes, prices and reviews.
-- Run after 01_db_project_identity.sql (product_reviews references customers).
-- Designed for an empty schema. Running twice will fail on existing tables.
BEGIN;

CREATE SCHEMA IF NOT EXISTS db_project_catalog;

CREATE TABLE IF NOT EXISTS db_project_catalog.brands (
    brand_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    brand_name VARCHAR(150) NOT NULL,
    description TEXT,
    website_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX uq_catalog_brands_name_ci ON db_project_catalog.brands (lower(brand_name));

CREATE TABLE IF NOT EXISTS db_project_catalog.product_categories (
    category_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_category_id BIGINT REFERENCES db_project_catalog.product_categories(category_id) ON DELETE RESTRICT,
    category_name VARCHAR(150) NOT NULL,
    category_slug VARCHAR(180) NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (parent_category_id IS NULL OR parent_category_id <> category_id)
);
CREATE INDEX idx_catalog_categories_parent ON db_project_catalog.product_categories(parent_category_id);
-- Preventing indirect category cycles requires an application check or trigger.

CREATE TABLE IF NOT EXISTS db_project_catalog.products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code VARCHAR(64) NOT NULL UNIQUE,
    brand_id BIGINT REFERENCES db_project_catalog.brands(brand_id) ON DELETE RESTRICT,
    product_name VARCHAR(255) NOT NULL,
    product_slug VARCHAR(280) NOT NULL UNIQUE,
    description TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'active', 'inactive', 'discontinued')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_catalog_products_brand ON db_project_catalog.products(brand_id);
CREATE INDEX idx_catalog_products_status ON db_project_catalog.products(status);

CREATE TABLE IF NOT EXISTS db_project_catalog.product_variants (
    variant_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES db_project_catalog.products(product_id) ON DELETE RESTRICT,
    sku VARCHAR(80) NOT NULL UNIQUE,
    barcode VARCHAR(80) UNIQUE,
    variant_name VARCHAR(255),
    weight_grams NUMERIC(12,3) CHECK (weight_grams >= 0),
    length_cm NUMERIC(12,3) CHECK (length_cm >= 0),
    width_cm NUMERIC(12,3) CHECK (width_cm >= 0),
    height_cm NUMERIC(12,3) CHECK (height_cm >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'inactive', 'discontinued')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, variant_id)
);
CREATE INDEX idx_catalog_variants_product ON db_project_catalog.product_variants(product_id);

CREATE TABLE IF NOT EXISTS db_project_catalog.product_category_assignments (
    product_id BIGINT NOT NULL REFERENCES db_project_catalog.products(product_id) ON DELETE CASCADE,
    category_id BIGINT NOT NULL REFERENCES db_project_catalog.product_categories(category_id) ON DELETE RESTRICT,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, category_id)
);
CREATE INDEX idx_catalog_assignments_category ON db_project_catalog.product_category_assignments(category_id);
CREATE UNIQUE INDEX uq_catalog_primary_category_per_product
    ON db_project_catalog.product_category_assignments(product_id) WHERE is_primary;

CREATE TABLE IF NOT EXISTS db_project_catalog.product_attributes (
    attribute_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attribute_code VARCHAR(80) NOT NULL UNIQUE,
    attribute_name VARCHAR(150) NOT NULL,
    data_type VARCHAR(20) NOT NULL
        CHECK (data_type IN ('text', 'number', 'boolean')),
    applies_to VARCHAR(20) NOT NULL
        CHECK (applies_to IN ('product', 'variant')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Typed values are stored as text, number, or boolean. The value type must match
-- product_attributes.data_type; that cross-table rule needs a trigger/application validation.
CREATE TABLE IF NOT EXISTS db_project_catalog.product_attribute_values (
    attribute_value_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attribute_id BIGINT NOT NULL REFERENCES db_project_catalog.product_attributes(attribute_id) ON DELETE RESTRICT,
    product_id BIGINT REFERENCES db_project_catalog.products(product_id) ON DELETE CASCADE,
    variant_id BIGINT REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE CASCADE,
    value_text TEXT,
    value_number NUMERIC(20,6),
    value_boolean BOOLEAN,
    CHECK (num_nonnulls(product_id, variant_id) = 1),
    CHECK (num_nonnulls(value_text, value_number, value_boolean) = 1)
);
CREATE UNIQUE INDEX uq_catalog_product_attribute ON db_project_catalog.product_attribute_values(product_id, attribute_id)
    WHERE product_id IS NOT NULL;
CREATE UNIQUE INDEX uq_catalog_variant_attribute ON db_project_catalog.product_attribute_values(variant_id, attribute_id)
    WHERE variant_id IS NOT NULL;
CREATE INDEX idx_catalog_attribute_values_attribute ON db_project_catalog.product_attribute_values(attribute_id);

CREATE TABLE IF NOT EXISTS db_project_catalog.product_images (
    image_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES db_project_catalog.products(product_id) ON DELETE CASCADE,
    variant_id BIGINT,
    image_url TEXT NOT NULL,
    alt_text VARCHAR(255),
    sort_order INTEGER NOT NULL DEFAULT 0 CHECK (sort_order >= 0),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id, variant_id)
        REFERENCES db_project_catalog.product_variants(product_id, variant_id) ON DELETE CASCADE
);
CREATE INDEX idx_catalog_images_product ON db_project_catalog.product_images(product_id, sort_order);
CREATE INDEX idx_catalog_images_variant ON db_project_catalog.product_images(variant_id);
CREATE UNIQUE INDEX uq_catalog_primary_product_image
    ON db_project_catalog.product_images(product_id) WHERE is_primary AND variant_id IS NULL;
CREATE UNIQUE INDEX uq_catalog_primary_variant_image
    ON db_project_catalog.product_images(variant_id) WHERE is_primary AND variant_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS db_project_catalog.price_lists (
    price_list_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    price_list_code VARCHAR(60) NOT NULL UNIQUE,
    price_list_name VARCHAR(150) NOT NULL,
    currency_code CHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    sales_channel VARCHAR(40) NOT NULL DEFAULT 'online',
    country_code CHAR(2) CHECK (country_code ~ '^[A-Z]{2}$'),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (price_list_id, currency_code)
);

-- Price history is effective-dated. Overlap prevention for the same SKU/list
-- should be enforced with an exclusion constraint or application transaction logic.
CREATE TABLE IF NOT EXISTS db_project_catalog.product_prices (
    product_price_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    variant_id BIGINT NOT NULL REFERENCES db_project_catalog.product_variants(variant_id) ON DELETE RESTRICT,
    price_list_id BIGINT NOT NULL REFERENCES db_project_catalog.price_lists(price_list_id) ON DELETE RESTRICT,
    amount NUMERIC(19,4) NOT NULL CHECK (amount >= 0),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    UNIQUE (variant_id, price_list_id, valid_from)
);
CREATE INDEX idx_catalog_prices_lookup ON db_project_catalog.product_prices
    (variant_id, price_list_id, valid_from DESC);

CREATE TABLE IF NOT EXISTS db_project_catalog.product_reviews (
    review_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES db_project_catalog.products(product_id) ON DELETE RESTRICT,
    customer_id BIGINT NOT NULL REFERENCES db_project_identity.customers(customer_id) ON DELETE RESTRICT,
    rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_title VARCHAR(200),
    review_body TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'published', 'rejected')),
    verified_purchase BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, customer_id)
);
CREATE INDEX idx_catalog_reviews_product_status ON db_project_catalog.product_reviews(product_id, status);
CREATE INDEX idx_catalog_reviews_customer ON db_project_catalog.product_reviews(customer_id);

CREATE TABLE IF NOT EXISTS db_project_catalog.product_status_history (
    status_history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES db_project_catalog.products(product_id) ON DELETE RESTRICT,
    old_status VARCHAR(20) CHECK (old_status IN ('draft', 'active', 'inactive', 'discontinued')),
    new_status VARCHAR(20) NOT NULL CHECK (new_status IN ('draft', 'active', 'inactive', 'discontinued')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    CHECK (old_status IS NULL OR old_status <> new_status)
);
CREATE INDEX idx_catalog_product_status_history ON db_project_catalog.product_status_history(product_id, changed_at DESC);

COMMIT;
