import json

from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET

from .models import Product
from .views import cart_snapshot, establish_guest


FREE_SHIPPING_THRESHOLD = 100_000
BASE_SHIPPING_FEE = 3_000

# 로컬 상품의 표시용 브랜드 샘플. 원본 카탈로그 연동 시 상품의 brand 값을 사용한다.
SAMPLE_BRANDS = {
    "air-earbuds": "샘플 오디오",
    "portable-speaker": "샘플 오디오",
    "mechanical-keyboard": "샘플 데스크",
    "wireless-mouse": "샘플 데스크",
}


def product_card(product):
    category = product.category or "카테고리 미제공"
    brand = getattr(product, "brand", None) or SAMPLE_BRANDS.get(product.pk, "브랜드 미제공")
    return {
        "id": product.pk,
        "label": f"{brand} · {category}",
        "category": category,
        "brand": brand,
        "price": product.price,
    }


def shipping_summary(total):
    remaining = max(0, FREE_SHIPPING_THRESHOLD - total)
    progress_pct = min(100, round((total / FREE_SHIPPING_THRESHOLD) * 100)) if FREE_SHIPPING_THRESHOLD else 100
    is_free = total >= FREE_SHIPPING_THRESHOLD
    shipping_fee = 0 if (total == 0 or is_free) else BASE_SHIPPING_FEE
    return {
        "threshold": FREE_SHIPPING_THRESHOLD,
        "base_fee": BASE_SHIPPING_FEE,
        "remaining": remaining,
        "progress_pct": progress_pct,
        "is_free": is_free,
        "shipping_fee": shipping_fee,
        "final_total": total + shipping_fee,
    }


def build_product_schema(card):
    return {
        "@context": "https://schema.org",
        "@type": "Product",
        "sku": card["id"],
        "name": card["label"],
        "category": card["category"],
        "brand": {"@type": "Brand", "name": card["brand"]},
        "offers": {
            "@type": "Offer",
            "priceCurrency": "KRW",
            "price": card["price"],
            "availability": "https://schema.org/InStock",
            "url": f"/store/products/{card['id']}/",
        },
    }


def build_catalog_json_ld(products):
    schema = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "데스크룸 전체 상품",
        "numberOfItems": len(products),
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": idx,
                "item": build_product_schema(card),
            }
            for idx, card in enumerate(products, start=1)
        ],
    }
    return json.dumps(schema, ensure_ascii=False)


def store_context(request, extra=None):
    current_guest = establish_guest(request)
    all_products = [product_card(product) for product in Product.objects.order_by("art")]
    product_map = {item["id"]: item for item in all_products}
    snapshot = cart_snapshot(current_guest)
    cart_lines = []
    for row in snapshot["items"]:
        card = product_map.get(row["product_id"])
        if card is None:
            continue
        cart_lines.append({
            "product": card,
            "quantity": row["quantity"],
            "subtotal": card["price"] * row["quantity"],
        })
    shipping = shipping_summary(snapshot["total"])
    context = {
        "all_products": all_products,
        "cart_lines": cart_lines,
        "cart_quantity": snapshot["quantity"],
        "cart_total": snapshot["total"],
        "shipping": shipping,
        "initial_cart": snapshot,
    }
    if extra:
        context.update(extra)
    return context


@require_GET
@never_cache
@ensure_csrf_cookie
def catalog(request):
    context = store_context(request)
    context["products"] = context["all_products"]
    context["json_ld"] = build_catalog_json_ld(context["all_products"])
    return render(request, "store_catalog.html", context)


@require_GET
@never_cache
@ensure_csrf_cookie
def detail(request, product_id):
    product = get_object_or_404(Product, pk=product_id)
    card = product_card(product)
    context = store_context(request, {"product": card})
    context["current_qty"] = next(
        (line["quantity"] for line in context["cart_lines"] if line["product"]["id"] == product.pk),
        0,
    )
    context["json_ld"] = json.dumps(build_product_schema(card), ensure_ascii=False)
    return render(request, "store_detail.html", context)


@require_GET
@never_cache
@ensure_csrf_cookie
def cart(request):
    context = store_context(request)
    context["lines"] = context["cart_lines"]
    context["total"] = context["cart_total"]
    return render(request, "store_cart.html", context)
