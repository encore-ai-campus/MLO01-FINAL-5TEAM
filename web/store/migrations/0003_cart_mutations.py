import django.db.models.deletion
from django.db import migrations, models


CONFIGURE_ZERO_START_SQL = """
DO $$
DECLARE
    line_seq text := pg_get_serial_sequence('store_cartline', 'id');
    action_seq text := pg_get_serial_sequence('store_cartaction', 'id');
BEGIN
    IF line_seq IS NOT NULL THEN
        EXECUTE format('ALTER SEQUENCE %s MINVALUE 0 START WITH 0', line_seq);
        IF NOT EXISTS (SELECT 1 FROM store_cartline) THEN
            EXECUTE format('ALTER SEQUENCE %s RESTART WITH 0', line_seq);
        END IF;
    END IF;
    IF action_seq IS NOT NULL THEN
        EXECUTE format('ALTER SEQUENCE %s MINVALUE 0 START WITH 0', action_seq);
        IF NOT EXISTS (SELECT 1 FROM store_cartaction) THEN
            EXECUTE format('ALTER SEQUENCE %s RESTART WITH 0', action_seq);
        END IF;
    END IF;
END $$;
"""


class Migration(migrations.Migration):
    dependencies = [("store", "0002_flow")]

    operations = [
        migrations.AddField(
            model_name="cart",
            name="revision",
            field=models.PositiveBigIntegerField(default=0),
        ),
        migrations.AlterField(
            model_name="cartaction",
            name="product",
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to="store.product"
            ),
        ),
        migrations.AddField(
            model_name="cartaction",
            name="action_type",
            field=models.CharField(default="add", max_length=12),
        ),
        migrations.AddField(
            model_name="cartaction",
            name="delta",
            field=models.IntegerField(default=1),
        ),
        migrations.AddField(
            model_name="cartaction",
            name="target_quantity",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.RunSQL(CONFIGURE_ZERO_START_SQL, reverse_sql=migrations.RunSQL.noop),
    ]
