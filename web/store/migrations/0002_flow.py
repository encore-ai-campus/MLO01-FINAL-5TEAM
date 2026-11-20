from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("store", "0001_initial")]
    operations = [
        migrations.AddField(
            model_name="cartaction", name="status",
            field=models.CharField(max_length=8, default="legacy"),
        ),
        migrations.AddField(
            model_name="cartaction", name="eligible_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name="cartaction", name="applied_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddIndex(
            model_name="cartaction",
            index=models.Index(fields=["status", "eligible_at"], name="store_delivery_ready"),
        ),
        migrations.AddField(
            model_name="cartaction", name="fault",
            field=models.CharField(max_length=16, default=""),
        ),
        migrations.AddField(
            model_name="cartaction", name="resumed_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name="cartaction",
            name="confirmed_at", field=models.DateTimeField(null=True),
        ),
    ]
