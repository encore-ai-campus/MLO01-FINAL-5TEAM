\set ON_ERROR_STOP on
\ir catalog.sql

BEGIN;
CREATE TEMP TABLE catalog_input (LIKE catalog.product_history INCLUDING CONSTRAINTS) ON COMMIT DROP;
\copy catalog_input FROM '/tmp/data08-product-history.csv' WITH (FORMAT csv, HEADER true, NULL '\N')

INSERT INTO catalog.product_history
SELECT * FROM catalog_input
ON CONFLICT (event_id) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM catalog_input i
        LEFT JOIN catalog.product_history h USING (event_id)
        WHERE h.event_id IS NULL OR
              (i.product_id, i.observed_at, i.source, i.event_seq, i.category_id, i.category_code, i.brand, i.price)
              IS DISTINCT FROM
              (h.product_id, h.observed_at, h.source, h.event_seq, h.category_id, h.category_code, h.brand, h.price)
    ) THEN
        RAISE EXCEPTION 'Catalog import conflicts with existing event IDs';
    END IF;
END
$$;
COMMIT;
ANALYZE catalog.product_history;
