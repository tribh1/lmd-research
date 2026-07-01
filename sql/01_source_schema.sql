CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS src_payment CASCADE;
DROP TABLE IF EXISTS src_order_item CASCADE;
DROP TABLE IF EXISTS src_order CASCADE;
DROP TABLE IF EXISTS src_product CASCADE;
DROP TABLE IF EXISTS src_customer CASCADE;
DROP TABLE IF EXISTS src_app_event CASCADE;
DROP TABLE IF EXISTS exp_ground_truth_violation CASCADE;

CREATE TABLE src_customer (
    customer_id BIGINT PRIMARY KEY,
    full_name TEXT,
    email TEXT,
    telephone TEXT,
    address TEXT,
    province TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE src_product (
    product_id BIGINT PRIMARY KEY,
    sku TEXT UNIQUE,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC(18,2),
    cost_price NUMERIC(18,2),
    status TEXT,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE src_order (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES src_customer(customer_id),
    order_date TIMESTAMP NOT NULL,
    order_status TEXT,
    channel TEXT,
    total_amount NUMERIC(18,2),
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE src_order_item (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT REFERENCES src_order(order_id),
    product_id BIGINT REFERENCES src_product(product_id),
    quantity INT,
    unit_price NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    line_amount NUMERIC(18,2),
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE src_payment (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT REFERENCES src_order(order_id),
    payment_method TEXT,
    card_number TEXT,
    amount NUMERIC(18,2),
    paid_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE src_app_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT,
    session_id TEXT,
    event_type TEXT,
    channel TEXT,
    page TEXT,
    amount NUMERIC(18,2),
    ingest_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE exp_ground_truth_violation (
    violation_id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_pk TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    expected_action TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Required for Debezium CDC
ALTER TABLE src_customer REPLICA IDENTITY FULL;
ALTER TABLE src_order REPLICA IDENTITY FULL;
ALTER TABLE src_order_item REPLICA IDENTITY FULL;
ALTER TABLE src_payment REPLICA IDENTITY FULL;
