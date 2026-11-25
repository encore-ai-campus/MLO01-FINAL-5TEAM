"""Shared action event columns in their stable storage order."""

ACTION_FIELDS = (
    ('event_id', 'string'),
    ('source', 'string'),
    ('event_seq', 'int64'),
    ('event_time', 'utc_timestamp_ms'),
    ('event_type', 'string'),
    ('product_id', 'int64'),
    ('category_id', 'int64'),
    ('category_code', 'string'),
    ('brand', 'string'),
    ('price', 'float64'),
    ('user_id', 'int64'),
    ('user_session', 'string'),
)

# The first three fields are created during conversion, not read from the CSV.
SOURCE_FIELDS = tuple(name for name, _ in ACTION_FIELDS[3:])
