"""Shared action event columns in their stable storage order."""

EVENT_ID = 'event_id'
SOURCE_FILE = 'source'
SOURCE_ROW = 'event_seq'
EVENT_TIME = 'event_time'
EVENT_TYPE = 'event_type'
PRODUCT_ID = 'product_id'
CATEGORY_ID = 'category_id'
CATEGORY_CODE = 'category_code'
BRAND = 'brand'
PRICE = 'price'
USER_ID = 'user_id'
USER_SESSION = 'user_session'
OBSERVED_TIME = 'observed_at'

# Kaggle CSV headers are fixed, independently of the published column names.
CSV_ACTION_FIELDS = (
    ('event_time', EVENT_TIME, 'utc_timestamp_ms'),
    ('event_type', EVENT_TYPE, 'string'),
    ('product_id', PRODUCT_ID, 'int64'),
    ('category_id', CATEGORY_ID, 'int64'),
    ('category_code', CATEGORY_CODE, 'string'),
    ('brand', BRAND, 'string'),
    ('price', PRICE, 'float64'),
    ('user_id', USER_ID, 'int64'),
    ('user_session', USER_SESSION, 'string'),
)
SOURCE_FIELDS = tuple(header for header, _, _ in CSV_ACTION_FIELDS)
ACTION_FIELDS = ((EVENT_ID, 'string'), (SOURCE_FILE, 'string'), (SOURCE_ROW, 'int64')) + tuple(
    (column, kind) for _, column, kind in CSV_ACTION_FIELDS)
