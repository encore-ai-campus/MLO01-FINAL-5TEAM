import json
from uuid import UUID, uuid4

from django.core.exceptions import ValidationError
from django.core.validators import validate_slug
from django.db import transaction
from django.db.models import F
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .models import Cart, CartAction, CartLine, Product


GUEST_COOKIE = "local_cart_guest"
GUEST_SALT = "store.guest-cart"


def guest_id(request):
    value = request.session.get("guest_id") or request.get_signed_cookie(
        GUEST_COOKIE, default=None, salt=GUEST_SALT,
    )
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


def establish_guest(request):
    current_guest = guest_id(request) or uuid4()
    if "guest_id" not in request.session:
        request.session["guest_id"] = str(current_guest)
    return current_guest


@transaction.atomic
def cart_snapshot(cart_id):
    if cart_id is None:
        return summarize_lines([], revision=0)
    revision = Cart.objects.select_for_update().filter(pk=cart_id).values_list("revision", flat=True).first()
    if revision is None:
        return summarize_lines([], revision=0)
    rows = CartLine.objects.filter(cart_id=cart_id).order_by("product__art").values_list(
        "product_id", "quantity", "product__price",
    )
    return summarize_lines(rows, revision=revision)


def summarize_lines(rows, revision=0):
    items = []
    quantity = 0
    total = 0
    for product_id, count, price in rows:
        items.append({"product_id": product_id, "quantity": count})
        quantity += count
        total += count * price
    return {"items": items, "quantity": quantity, "total": total, "revision": revision}


def api_error(message, status):
    return JsonResponse({"error": message}, status=status)


def csrf_failure(request, reason=""):
    return api_error("요청을 확인할 수 없습니다. 화면을 새로고침한 뒤 다시 시도해 주세요.", 403)


@require_GET
@never_cache
@ensure_csrf_cookie
def cart_detail(request):
    return JsonResponse(cart_snapshot(establish_guest(request)))


def parse_json_request(request, required, missing_msg="필수 요청 값을 확인해 주세요."):
    current_guest = guest_id(request)
    if current_guest is None:
        return None, api_error("상점 화면을 다시 열어 장바구니를 시작해 주세요.", 403)
    if request.content_type != "application/json":
        return None, api_error("JSON 형식으로 요청해 주세요.", 415)
    try:
        payload = json.loads(request.body)
    except (ValueError, RecursionError):
        return None, api_error("요청 본문의 JSON 형식을 확인해 주세요.", 400)
    if not isinstance(payload, dict) or payload.keys() != required:
        return None, api_error(missing_msg, 400)
    event_value = payload["event_id"]
    if not isinstance(event_value, str):
        return None, api_error("이벤트 ID 형식을 확인해 주세요.", 400)
    try:
        event_id = UUID(event_value)
    except ValueError:
        return None, api_error("이벤트 ID 형식을 확인해 주세요.", 400)
    return (current_guest, payload, event_id), None


def validate_product(product_id):
    if not isinstance(product_id, str) or len(product_id) > 50:
        return api_error("상품 ID 형식을 확인해 주세요.", 400)
    try:
        validate_slug(product_id)
    except ValidationError:
        return api_error("상품 ID 형식을 확인해 주세요.", 400)
    if not Product.objects.filter(pk=product_id).exists():
        return api_error("상품을 찾을 수 없습니다.", 404)
    return None


@require_POST
def cart_add(request):
    required = {"product_id", "event_id"}
    parsed, err = parse_json_request(
        request, required, missing_msg="product_id와 event_id를 함께 보내 주세요.",
    )
    if err is not None:
        return err
    current_guest, payload, event_id = parsed
    product_id = payload["product_id"]
    product_err = validate_product(product_id)
    if product_err is not None:
        return product_err

    return commit_add(current_guest, event_id, product_id)


@transaction.atomic
def commit_add(current_guest, event_id, product_id):
    # 같은 방문자의 모든 담기는 이 행을 잠급니다. 첫 담기는 UUID 기본 키가 직렬화합니다.
    cart, _ = Cart.objects.select_for_update().get_or_create(pk=current_guest)
    action, created = CartAction.objects.get_or_create(
        cart=cart, event_id=event_id, defaults={
            "action_type": "add", "product_id": product_id, "delta": 1, "target_quantity": None,
            "status": "pending", "eligible_at": timezone.now(),
        },
    )
    if action.action_type != "add" or action.product_id != product_id:
        return api_error("이미 다른 요청에 사용한 이벤트 ID입니다.", 409)
    if created:
        lines = CartLine.objects.filter(cart=cart, product_id=product_id)
        if not lines.update(quantity=F("quantity") + 1):
            CartLine.objects.create(cart=cart, product_id=product_id)
        cart.revision += 1
        cart.save(update_fields=["revision"])
    state = cart_snapshot(cart.pk)
    return JsonResponse({"cart": state, "replayed": not created, "event_id": str(event_id)})


@require_POST
def cart_update(request):
    parsed, err = parse_json_request(
        request, {"product_id", "quantity", "event_id"},
        missing_msg="product_id, quantity, event_id를 함께 보내 주세요.",
    )
    if err is not None:
        return err
    current_guest, payload, event_id = parsed
    quantity = payload["quantity"]
    if type(quantity) is not int or not (0 <= quantity <= 99):
        return api_error("수량은 0 이상 99 이하의 정수로 입력해 주세요.", 400)
    product_id = payload["product_id"]
    product_err = validate_product(product_id)
    if product_err is not None:
        return product_err

    with transaction.atomic():
        cart, _ = Cart.objects.select_for_update().get_or_create(pk=current_guest)
        existing = CartAction.objects.filter(cart=cart, event_id=event_id).first()
        if existing is not None:
            if (
                existing.action_type != "update"
                or existing.product_id != product_id
                or existing.target_quantity != quantity
            ):
                return api_error("이미 다른 요청에 사용한 이벤트 ID입니다.", 409)
            return JsonResponse({"cart": cart_snapshot(cart.pk), "replayed": True, "event_id": str(event_id)})

        line = CartLine.objects.filter(cart=cart, product_id=product_id).first()
        old_qty = line.quantity if line else 0
        delta = quantity - old_qty
        if quantity == 0:
            if line is not None:
                line.delete()
        elif line is not None:
            line.quantity = quantity
            line.save(update_fields=["quantity"])
        else:
            CartLine.objects.create(cart=cart, product_id=product_id, quantity=quantity)

        CartAction.objects.create(
            cart=cart, event_id=event_id, action_type="update", product_id=product_id,
            delta=delta, target_quantity=quantity, status="pending",
            eligible_at=timezone.now(),
        )
        cart.revision += 1
        cart.save(update_fields=["revision"])
        state = cart_snapshot(cart.pk)
    return JsonResponse({"cart": state, "replayed": False, "event_id": str(event_id)})


@require_POST
def cart_remove(request):
    parsed, err = parse_json_request(
        request, {"product_id", "event_id"}, missing_msg="product_id와 event_id를 함께 보내 주세요.",
    )
    if err is not None:
        return err
    current_guest, payload, event_id = parsed
    product_id = payload["product_id"]
    product_err = validate_product(product_id)
    if product_err is not None:
        return product_err

    with transaction.atomic():
        cart, _ = Cart.objects.select_for_update().get_or_create(pk=current_guest)
        existing = CartAction.objects.filter(cart=cart, event_id=event_id).first()
        if existing is not None:
            if existing.action_type != "remove" or existing.product_id != product_id:
                return api_error("이미 다른 요청에 사용한 이벤트 ID입니다.", 409)
            return JsonResponse({"cart": cart_snapshot(cart.pk), "replayed": True, "event_id": str(event_id)})

        line = CartLine.objects.filter(cart=cart, product_id=product_id).first()
        old_qty = line.quantity if line else 0
        if line is not None:
            line.delete()

        CartAction.objects.create(
            cart=cart, event_id=event_id, action_type="remove", product_id=product_id,
            delta=-old_qty, target_quantity=0, status="pending",
            eligible_at=timezone.now(),
        )
        cart.revision += 1
        cart.save(update_fields=["revision"])
        state = cart_snapshot(cart.pk)
    return JsonResponse({"cart": state, "replayed": False, "event_id": str(event_id)})


@require_POST
def cart_clear(request):
    parsed, err = parse_json_request(
        request, {"event_id"}, missing_msg="event_id를 보내 주세요.",
    )
    if err is not None:
        return err
    current_guest, _, event_id = parsed

    with transaction.atomic():
        cart, _ = Cart.objects.select_for_update().get_or_create(pk=current_guest)
        existing = CartAction.objects.filter(cart=cart, event_id=event_id).first()
        if existing is not None:
            if existing.action_type != "clear":
                return api_error("이미 다른 요청에 사용한 이벤트 ID입니다.", 409)
            return JsonResponse({"cart": cart_snapshot(cart.pk), "replayed": True, "event_id": str(event_id)})

        old_total = sum(CartLine.objects.filter(cart=cart).values_list("quantity", flat=True))
        CartLine.objects.filter(cart=cart).delete()
        CartAction.objects.create(
            cart=cart, event_id=event_id, action_type="clear", product=None,
            delta=-old_total, target_quantity=0, status="pending",
            eligible_at=timezone.now(),
        )
        cart.revision += 1
        cart.save(update_fields=["revision"])
        state = cart_snapshot(cart.pk)
    return JsonResponse({"cart": state, "replayed": False, "event_id": str(event_id)})
