import uuid

from django.db import models


class Product(models.Model):
    id = models.SlugField(primary_key=True)
    name = models.CharField(max_length=80)
    category = models.CharField(max_length=40)
    price = models.PositiveIntegerField()
    art = models.PositiveSmallIntegerField(unique=True)
    description = models.CharField(max_length=200)


class Cart(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    revision = models.PositiveBigIntegerField(default=0)


class CartLine(models.Model):
    cart = models.ForeignKey(Cart, on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.PROTECT)
    quantity = models.PositiveIntegerField(default=1)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["cart", "product"], name="store_line_cart_product_uniq"),
            models.CheckConstraint(condition=models.Q(quantity__gte=1), name="store_line_quantity_gte_one"),
        ]


class CartAction(models.Model):
    cart = models.ForeignKey(Cart, on_delete=models.CASCADE)
    event_id = models.UUIDField()
    action_type = models.CharField(max_length=12, default="add")
    product = models.ForeignKey(Product, on_delete=models.PROTECT, null=True, blank=True)
    delta = models.IntegerField(default=1)
    target_quantity = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=8, default="legacy")
    eligible_at = models.DateTimeField(null=True)
    applied_at = models.DateTimeField(null=True)
    fault = models.CharField(max_length=16, default="")
    resumed_at = models.DateTimeField(null=True)
    confirmed_at = models.DateTimeField(null=True)

    class Meta:
        indexes = [models.Index(fields=["status", "eligible_at"], name="store_delivery_ready")]
        constraints = [
            models.UniqueConstraint(fields=["cart", "event_id"], name="store_action_cart_event_uniq"),
        ]
