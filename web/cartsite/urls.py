from django.conf import settings
from django.urls import path
from django.views.generic import RedirectView
from django.views.static import serve

from store import storefront, views


urlpatterns = [
    path("store/", storefront.catalog, name="store-catalog"),
    path("store/products/<slug:product_id>/", storefront.detail, name="store-product"),
    path("store/cart/", storefront.cart, name="store-cart"),
    path("", RedirectView.as_view(pattern_name="store-catalog", permanent=False)),
    path("api/cart/", views.cart_detail, name="cart-detail"),
    path("api/cart/add/", views.cart_add, name="cart-add"),
    path("api/cart/update/", views.cart_update, name="cart-update"),
    path("api/cart/remove/", views.cart_remove, name="cart-remove"),
    path("api/cart/clear/", views.cart_clear, name="cart-clear"),
    # 로컬에서 실행하는 이 화면의 정적 파일은 static 디렉터리에서만 제공합니다.
    path("static/<path:path>", serve, {"document_root": settings.BASE_DIR / "static"}),
]
