CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS public.users (
    id integer PRIMARY KEY,
    full_name varchar(100) NOT NULL,
    email varchar(200),
    created_at timestamp NOT NULL
) DISTSTYLE KEY DISTKEY (id) SORTKEY (id);
COMMENT ON TABLE public.users IS 'Demo customers';
COMMENT ON COLUMN public.users.id IS 'Customer identifier';
CREATE TABLE IF NOT EXISTS analytics.orders (
    id bigint PRIMARY KEY,
    user_id integer REFERENCES public.users(id),
    amount decimal(30,9) NOT NULL,
    ordered_at timestamp NOT NULL
) DISTSTYLE KEY DISTKEY (user_id) SORTKEY (ordered_at);
CREATE TABLE IF NOT EXISTS analytics.events (
    id bigint PRIMARY KEY,
    payload SUPER
) DISTSTYLE EVEN;
CREATE TABLE IF NOT EXISTS analytics.metadata_types (
    tenant_id integer NOT NULL,
    type_id integer NOT NULL,
    amount decimal(30,9) DEFAULT 123.456789012,
    "MixedCase" varchar(12),
    nullable_text varchar(20),
    PRIMARY KEY (tenant_id, type_id)
);
COMMENT ON COLUMN analytics.metadata_types.amount IS 'Exact test amount';
CREATE TABLE IF NOT EXISTS analytics.type_references (
    id integer PRIMARY KEY,
    tenant_id integer,
    type_id integer,
    FOREIGN KEY (tenant_id, type_id) REFERENCES analytics.metadata_types(tenant_id, type_id)
);
CREATE OR REPLACE VIEW analytics.daily_sales AS
    SELECT ordered_at::date AS day, sum(amount) AS amount
    FROM analytics.orders GROUP BY ordered_at::date;
INSERT INTO public.users
SELECT 1, 'Alice', 'alice@example.test', '2026-01-02 03:04:05'::timestamp
WHERE NOT EXISTS (SELECT 1 FROM public.users WHERE id = 1);
INSERT INTO public.users
SELECT 2, 'Bob', NULL, '2026-01-03 04:05:06'::timestamp
WHERE NOT EXISTS (SELECT 1 FROM public.users WHERE id = 2);
INSERT INTO analytics.orders
SELECT 10, 1, 123.456789012, '2026-01-02 12:00:00'::timestamp
WHERE NOT EXISTS (SELECT 1 FROM analytics.orders WHERE id = 10);
INSERT INTO analytics.orders
SELECT 11, 2, 0.000000001, '2026-01-03 12:00:00'::timestamp
WHERE NOT EXISTS (SELECT 1 FROM analytics.orders WHERE id = 11);
INSERT INTO analytics.events
SELECT 1, JSON_PARSE('{"customer":{"name":"Alice"},"age":30,"items":[{"sku":"a","qty":2},{"sku":"b","qty":3}],"nullable":null,"precise":12345678901234567890.123456789}')
WHERE NOT EXISTS (SELECT 1 FROM analytics.events WHERE id = 1);
