CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.product_history (
    product_id bigint NOT NULL,
    observed_at timestamptz NOT NULL,
    source text COLLATE "C" NOT NULL,
    event_seq bigint NOT NULL CHECK (event_seq >= 0),
    event_id text NOT NULL,
    category_id bigint,
    category_code text,
    brand text,
    price double precision,
    PRIMARY KEY (product_id, observed_at, source, event_seq),
    UNIQUE (event_id)
);

CREATE OR REPLACE FUNCTION catalog.product_at(requested_id bigint, requested_at timestamptz)
RETURNS SETOF catalog.product_history LANGUAGE sql STABLE AS $$
    SELECT h.*
    FROM catalog.product_history h
    WHERE h.product_id = requested_id AND h.observed_at <= requested_at
    ORDER BY h.observed_at DESC, h.source DESC, h.event_seq DESC
    LIMIT 1
$$;

CREATE OR REPLACE FUNCTION catalog.product_at_event(
    requested_id bigint, requested_at timestamptz, requested_source text, requested_event_seq bigint
)
RETURNS SETOF catalog.product_history LANGUAGE sql STABLE AS $$
    SELECT h.*
    FROM catalog.product_history h
    WHERE h.product_id = requested_id
      AND (h.observed_at, h.source, h.event_seq) <= (requested_at, requested_source COLLATE "C", requested_event_seq)
    ORDER BY h.observed_at DESC, h.source DESC, h.event_seq DESC
    LIMIT 1
$$;
