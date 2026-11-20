"use strict";

(() => {
  const {
    uuidPattern,
    won,
    newEvent,
    getCsrfToken,
    requestJson,
    publishSync,
    subscribeSync,
  } = window.Deskroom;

  const FREE_SHIPPING_THRESHOLD = 100000;
  const BASE_SHIPPING_FEE = 3000;

  const byId = (id) => document.getElementById(id);
  const products = new Map(
    JSON.parse(byId("store-products")?.textContent || "[]").map((product) => [product.id, product])
  );
  const initialCartNode = byId("store-initial-cart");
  const requestKey = "deskroom.cart.requests.v1";
  const pending = new Map();
  const inFlight = new Map();

  let currentCart = initialCartNode ? JSON.parse(initialCartNode.textContent) : null;
  let cartGeneration = 0;
  let cartMutating = false;
  let noticeTimer;

  const catalogGrid = byId("store-catalog-grid");
  const cartItems = byId("store-cart-items");
  const detailState = byId("detail-cart-state");
  const addStatus = byId("add-status");
  const cartSheet = byId("store-cart-sheet");
  const sheetBackdrop = byId("store-sheet-backdrop");
  const sheetOpenBtn = byId("store-sheet-open");
  const sheetCloseBtn = byId("store-sheet-close");
  const checkoutStatus = byId("store-checkout-status");

  function rememberRequests() {
    try {
      sessionStorage.setItem(requestKey, JSON.stringify({
        pending: Object.fromEntries(pending),
      }));
    } catch {
      // The active tab keeps request IDs in memory when storage is disabled.
    }
  }

  function restoreRequests() {
    try {
      const saved = JSON.parse(sessionStorage.getItem(requestKey)) || {};
      for (const [id, productId] of Object.entries(saved.pending || {})) {
        if (products.has(productId) && uuidPattern.test(id)) pending.set(id, productId);
      }
    } catch {
      // A fresh tab starts with no pending requests.
    }
  }

  function notify(message) {
    const notice = byId("store-notice");
    const announcer = byId("store-announcer");
    if (announcer) announcer.replaceChildren(document.createTextNode(message));
    if (!notice) return;
    notice.textContent = message;
    notice.classList.add("is-visible");
    window.clearTimeout(noticeTimer);
    noticeTimer = window.setTimeout(() => notice.classList.remove("is-visible"), 4500);
  }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function openCartSheet() {
    if (!cartSheet) {
      byId("store-cart-panel")?.scrollIntoView({ behavior: "smooth" });
      return;
    }
    cartSheet.classList.add("is-sheet-open");
    if (sheetBackdrop) sheetBackdrop.hidden = false;
    if (sheetOpenBtn) {
      sheetOpenBtn.setAttribute("aria-expanded", "true");
      sheetOpenBtn.textContent = "장바구니 닫기 ▼";
    }
  }

  function closeCartSheet() {
    if (!cartSheet) return;
    cartSheet.classList.remove("is-sheet-open");
    if (sheetBackdrop) sheetBackdrop.hidden = true;
    if (sheetOpenBtn) {
      sheetOpenBtn.setAttribute("aria-expanded", "false");
      sheetOpenBtn.textContent = "장바구니 열기 ▲";
    }
  }

  function toggleCartSheet() {
    if (!cartSheet) {
      byId("store-cart-panel")?.scrollIntoView({ behavior: "smooth" });
      return;
    }
    if (cartSheet.classList.contains("is-sheet-open")) closeCartSheet();
    else openCartSheet();
  }

  function updateShippingAndSticky(total, quantity) {
    const remaining = Math.max(0, FREE_SHIPPING_THRESHOLD - total);
    const pct = Math.min(100, Math.round((total / FREE_SHIPPING_THRESHOLD) * 100));
    const isFree = total >= FREE_SHIPPING_THRESHOLD;
    const shippingFee = (total === 0 || isFree) ? 0 : BASE_SHIPPING_FEE;
    const finalTotal = total + shippingFee;

    const shipText = byId("store-shipping-text");
    if (shipText) {
      shipText.replaceChildren();
      if (isFree) {
        shipText.append(element("strong", "", "무료배송 혜택이 적용되었습니다 ✓"));
      } else {
        const strong = element("strong", "", won.format(remaining) + "원");
        const bold = element("b", "", "무료배송");
        shipText.append(strong, document.createTextNode(" 추가 시 "), bold, document.createTextNode(" (100,000원 이상 무료)"));
      }
    }
    const shipPct = byId("store-shipping-pct");
    if (shipPct) shipPct.textContent = pct + "%";
    const shipBar = byId("store-shipping-bar");
    if (shipBar) shipBar.setAttribute("aria-valuenow", String(pct));
    const shipFill = byId("store-shipping-fill");
    if (shipFill) shipFill.style.width = pct + "%";

    const shipFeeNode = byId("store-shipping-fee");
    if (shipFeeNode) {
      shipFeeNode.textContent = shippingFee === 0 ? "무료 (0원)" : "+" + won.format(shippingFee) + "원";
    }
    const finalTotalNode = byId("store-final-total");
    if (finalTotalNode) finalTotalNode.textContent = won.format(finalTotal);

    const stickyQty = byId("store-sticky-qty");
    if (stickyQty) stickyQty.textContent = won.format(quantity);
    const stickyTotal = byId("store-sticky-total");
    if (stickyTotal) stickyTotal.textContent = won.format(total) + "원";
    const stickyShip = byId("store-sticky-shipping");
    if (stickyShip) {
      stickyShip.textContent = isFree ? "무료배송" : won.format(remaining) + "원 추가 시 무료배송";
    }
  }

  function cartLine(item) {
    const product = products.get(item.product_id);
    const name = product ? product.label : item.product_id;
    const row = element("li", "cart-item");
    const info = element("div", "cart-item-info");
    const top = element("div", "cart-item-top");
    const titleWrap = document.createElement("div");
    const link = element("a", "cart-item-name", name);
    link.href = "/store/products/" + encodeURIComponent(item.product_id) + "/";
    titleWrap.append(link);
    if (product) {
      titleWrap.append(element("span", "cart-unit-price", "개당 " + won.format(product.price) + "원"));
    }

    const remove = element("button", "remove-line-btn", "담기 취소");
    remove.type = "button";
    remove.dataset.remove = item.product_id;
    top.append(titleWrap, remove);

    const controls = element("div", "cart-item-controls");
    const stepper = element("div", "qty-stepper");
    const dec = element("button", "qty-btn", "−");
    dec.type = "button";
    dec.dataset.dec = item.product_id;
    dec.setAttribute("aria-label", name + " 수량 1 줄이기");

    const input = element("input", "qty-input");
    input.type = "number";
    input.min = "0";
    input.max = "99";
    input.value = String(item.quantity);
    input.dataset.qtyInput = item.product_id;
    input.setAttribute("aria-label", name + " 수량");

    const inc = element("button", "qty-btn", "+");
    inc.type = "button";
    inc.dataset.inc = item.product_id;
    inc.setAttribute("aria-label", name + " 수량 1 늘리기");
    stepper.append(dec, input, inc);

    const linePrice = product ? won.format(item.quantity * product.price) + "원" : "";
    controls.append(stepper, element("span", "cart-line-price", linePrice));

    info.append(top, controls);
    row.append(info);
    return row;
  }

  function acceptCart(cart, options = {}) {
    if (!cart || !Number.isSafeInteger(cart.revision) || !Number.isSafeInteger(cart.quantity)
        || !Number.isSafeInteger(cart.total) || !Array.isArray(cart.items)) {
      throw new Error("장바구니 응답을 읽을 수 없습니다.");
    }
    if (!options.force && currentCart && cart.revision < currentCart.revision) return;
    currentCart = cart;
    const quantityText = won.format(cart.quantity);

    const navQty = byId("store-nav-qty");
    if (navQty) navQty.textContent = quantityText;
    const shortcut = document.querySelector(".cart-shortcut");
    if (shortcut) shortcut.setAttribute("aria-label", "장바구니, 상품 " + quantityText + "개");

    if (cartItems) {
      byId("store-cart-quantity").textContent = quantityText + "개";
      byId("store-clear-cart").hidden = cart.items.length === 0;
      byId("store-cart-total").textContent = won.format(cart.total);
      cartItems.replaceChildren(...cart.items.map(cartLine));
      cartItems.hidden = cart.items.length === 0;
      byId("store-cart-placeholder").hidden = !cartItems.hidden;
      byId("store-placeholder-title").textContent = "장바구니가 비어 있습니다.";
      byId("store-placeholder-help").textContent = "원하시는 상품을 장바구니에 담아 보세요.";
      byId("store-reload-cart").hidden = true;
    }

    updateShippingAndSticky(cart.total, cart.quantity);

    if (checkoutStatus && cart.items.length === 0) {
      checkoutStatus.hidden = true;
      checkoutStatus.textContent = "";
    }

    if (detailState) {
      const productId = detailState.dataset.detailProduct;
      const line = cart.items.find((row) => row.product_id === productId);
      detailState.hidden = !line;
      const qtyInput = detailState.querySelector("[data-qty-input]");
      if (qtyInput && line) qtyInput.value = String(line.quantity);
    }
  }

  function awaitingRetry(productId) {
    const events = [];
    for (const [id, owner] of pending) {
      if (!inFlight.has(id) && (!productId || owner === productId)) {
        events.push({ product_id: owner, event_id: id });
      }
    }
    return events;
  }

  function renderControls() {
    const busyProducts = new Set(inFlight.values());
    const retryable = awaitingRetry();
    const retryProducts = new Set(retryable.map((event) => event.product_id));

    for (const button of document.querySelectorAll("[data-store-add]")) {
      const product = products.get(button.dataset.storeAdd);
      if (!product) continue;
      const defaultLabel = button.dataset.defaultLabel || "담기";
      const label = retryProducts.has(product.id) ? "다시 시도" : defaultLabel;
      const busy = busyProducts.has(product.id);
      button.disabled = busy;
      button.setAttribute("aria-busy", String(busy));
      const labelNode = button.querySelector("[data-add-label]");
      if (labelNode) labelNode.textContent = label;
      button.setAttribute("aria-label", product.label + " " + label);
    }

    const pendingBar = byId("store-pending-bar");
    if (pendingBar) {
      pendingBar.hidden = !retryable.length;
      byId("store-pending-message").textContent = "응답을 확인하지 못한 요청이 " + retryable.length + "건 있습니다.";
    }
  }

  async function refreshCart(announceFailure = false, force = false) {
    const generation = cartGeneration;
    try {
      const data = await requestJson("/api/cart/");
      if (generation !== cartGeneration || cartMutating || inFlight.size > 0) return;
      acceptCart(data, { force: force || data.revision === 0 });
    } catch {
      if (cartItems && !currentCart) {
        byId("store-placeholder-title").textContent = "장바구니를 불러오지 못했습니다.";
        byId("store-placeholder-help").textContent = "연결을 확인하고 다시 시도해 주세요.";
        byId("store-reload-cart").hidden = false;
      }
      if (announceFailure) notify("장바구니를 불러오지 못했습니다. 연결을 확인해 주세요.");
    }
  }

  async function deliver(event) {
    try {
      const result = await requestJson("/api/cart/add/", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRFToken": getCsrfToken() },
        body: JSON.stringify(event),
      });
      if (result.event_id !== event.event_id || typeof result.replayed !== "boolean") {
        throw new Error("요청 결과를 확인할 수 없습니다.");
      }
      acceptCart(result.cart);
      pending.delete(event.event_id);
      return { ok: true, replayed: result.replayed };
    } catch (error) {
      if ([400, 404, 409].includes(error.status)) {
        pending.delete(event.event_id);
        return { ok: false, message: error.message };
      }
      if (error.status === 403) {
        return { ok: false, message: "보안 확인이 필요합니다. 새로고침한 뒤 같은 요청으로 다시 보내 주세요." };
      }
      return { ok: false, message: "응답을 확인하지 못했습니다. 같은 요청으로 다시 보내 주세요." };
    } finally {
      inFlight.delete(event.event_id);
      rememberRequests();
    }
  }

  async function submit(events, action) {
    const sending = events.filter((event) => !inFlight.has(event.event_id));
    if (!sending.length) return;
    cartGeneration++;
    const productId = sending[0].product_id;
    const productName = products.get(productId)?.label || productId;
    for (const event of sending) {
      pending.set(event.event_id, event.product_id);
      inFlight.set(event.event_id, event.product_id);
    }
    rememberRequests();
    renderControls();
    if (addStatus && detailState?.dataset.detailProduct === productId) {
      addStatus.textContent = "담는 중…";
    }

    const results = await Promise.all(sending.map(deliver));
    const failed = results.filter((result) => !result.ok);
    const added = results.filter((result) => result.ok && !result.replayed).length;
    let message;
    if (failed.length) {
      const confirmed = results.length - failed.length;
      message = (confirmed ? confirmed + "건을 확인했습니다. " : "") + failed[0].message;
      void refreshCart();
    } else if (action === "retry") {
      message = results.length + "건의 요청 결과를 확인했습니다. 새로 담긴 수량은 " + added + "개입니다.";
    } else if (!added) {
      message = "이미 반영된 요청입니다. 추가로 담긴 수량은 0개입니다.";
    } else {
      message = productName + " 1개를 담았습니다.";
    }

    renderControls();
    notify(message);
    if (addStatus && detailState?.dataset.detailProduct === productId) {
      const line = currentCart?.items.find((row) => row.product_id === productId);
      addStatus.textContent = failed.length
        ? message
        : "장바구니에 이 상품이 " + (line ? line.quantity : 1) + "개 담겨 있습니다.";
    }
    publishSync({
      type: "cart:action",
      events: sending.flatMap((event, i) => results[i].ok ? [{ ...event, replayed: results[i].replayed }] : []),
      message,
      revision: currentCart?.revision,
    });
  }

  async function mutateCart(url, payload, message) {
    if (cartMutating) return;
    cartMutating = true;
    cartGeneration++;
    const eventId = newEvent(payload.product_id || "").event_id;
    try {
      const result = await requestJson(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRFToken": getCsrfToken() },
        body: JSON.stringify({ ...payload, event_id: eventId }),
      });
      if (result.event_id !== eventId) throw new Error("요청 결과를 확인할 수 없습니다.");
      acceptCart(result.cart);
      if (addStatus && (!payload.product_id || detailState?.dataset.detailProduct === payload.product_id)) {
        addStatus.textContent = message;
      }
      notify(message);
      publishSync({
        type: "cart:action",
        events: [{ product_id: payload.product_id || "", event_id: eventId, replayed: result.replayed }],
        message,
        revision: result.cart.revision,
      });
    } catch (error) {
      notify(error.message || "장바구니 변경에 실패했습니다.");
      void refreshCart(false, true);
    } finally {
      cartMutating = false;
    }
  }

  function handleLineClick(event) {
    const decBtn = event.target.closest("[data-dec]");
    if (decBtn) {
      const productId = decBtn.dataset.dec;
      const item = currentCart?.items.find((row) => row.product_id === productId);
      if (!item) return;
      const name = products.get(productId)?.label || productId;
      if (item.quantity <= 1) {
        void mutateCart("/api/cart/remove/", { product_id: productId }, name + " 담기를 취소했습니다.");
      } else {
        const next = item.quantity - 1;
        void mutateCart("/api/cart/update/", { product_id: productId, quantity: next }, name + " 수량을 " + next + "개로 변경했습니다.");
      }
      return;
    }

    const incBtn = event.target.closest("[data-inc]");
    if (incBtn) {
      const productId = incBtn.dataset.inc;
      const item = currentCart?.items.find((row) => row.product_id === productId);
      if (!item || item.quantity >= 99) return;
      const name = products.get(productId)?.label || productId;
      const next = item.quantity + 1;
      void mutateCart("/api/cart/update/", { product_id: productId, quantity: next }, name + " 수량을 " + next + "개로 변경했습니다.");
      return;
    }

    const removeBtn = event.target.closest("[data-remove]");
    if (removeBtn) {
      const productId = removeBtn.dataset.remove;
      const name = products.get(productId)?.label || productId;
      void mutateCart("/api/cart/remove/", { product_id: productId }, name + " 담기를 취소했습니다.");
    }
  }

  function handleQtyChange(event) {
    const input = event.target.closest("[data-qty-input]");
    if (!input) return;
    const productId = input.dataset.qtyInput;
    const item = currentCart?.items.find((row) => row.product_id === productId);
    const name = products.get(productId)?.label || productId;
    let next = Number.parseInt(input.value, 10);
    if (!Number.isFinite(next)) {
      input.value = String(item ? item.quantity : 1);
      return;
    }
    next = Math.max(0, Math.min(99, next));
    input.value = String(next);
    if (item && next === item.quantity) return;
    if (next === 0) {
      void mutateCart("/api/cart/remove/", { product_id: productId }, name + " 담기를 취소했습니다.");
    } else {
      void mutateCart("/api/cart/update/", { product_id: productId, quantity: next }, name + " 수량을 " + next + "개로 변경했습니다.");
    }
  }

  function setUpCatalog() {
    const filters = byId("store-category-filters");
    if (!filters || !catalogGrid) return;
    const items = Array.from(catalogGrid.querySelectorAll(".product-grid-item, .product-card")).filter(
      (node) => node.parentElement === catalogGrid
    );
    for (const category of new Set(Array.from(products.values(), (product) => product.category))) {
      const button = element("button", "filter-button", category);
      button.type = "button";
      button.dataset.category = category;
      button.setAttribute("aria-pressed", "false");
      filters.append(button);
    }
    filters.addEventListener("click", (event) => {
      const selected = event.target.closest(".filter-button");
      if (!selected) return;
      for (const button of filters.children) button.setAttribute("aria-pressed", String(button === selected));
      let count = 0;
      for (const item of items) {
        const product = products.get(item.dataset.productId);
        item.hidden = Boolean(selected.dataset.category && product?.category !== selected.dataset.category);
        if (!item.hidden) count += 1;
      }
      byId("store-product-count").textContent = "총 " + count + "개";
    });

    const sortGroup = byId("store-sort-chips");
    if (sortGroup) {
      sortGroup.addEventListener("click", (event) => {
        const chip = event.target.closest("[data-sort]");
        if (!chip) return;
        for (const btn of sortGroup.querySelectorAll("[data-sort]")) {
          btn.setAttribute("aria-pressed", String(btn === chip));
        }
        const mode = chip.dataset.sort;
        const sorted = [...items].sort((a, b) => {
          const pa = products.get(a.dataset.productId);
          const pb = products.get(b.dataset.productId);
          if (mode === "price-asc") return (pa?.price || 0) - (pb?.price || 0);
          if (mode === "price-desc") return (pb?.price || 0) - (pa?.price || 0);
          return 0;
        });
        catalogGrid.append(...sorted);
      });
    }

    const viewToggle = byId("store-view-toggle");
    if (viewToggle) {
      viewToggle.addEventListener("click", () => {
        const nextLayout = catalogGrid.dataset.layout === "1col" ? "4col" : "1col";
        catalogGrid.dataset.layout = nextLayout;
        viewToggle.textContent = nextLayout === "1col" ? "카드형" : "리스트형";
      });
    }
  }

  document.addEventListener("click", (event) => {
    const payBtn = event.target.closest("[data-quick-pay]");
    if (payBtn) {
      const provider = payBtn.dataset.quickPay;
      if (!currentCart || currentCart.quantity === 0) {
        notify("장바구니가 비어 있습니다. 먼저 상품을 담아 주세요.");
        return;
      }
      const isFree = currentCart.total >= FREE_SHIPPING_THRESHOLD;
      const shippingFee = isFree ? 0 : BASE_SHIPPING_FEE;
      const finalAmount = currentCart.total + shippingFee;
      const msg = "[" + provider + "] 상품 " + currentCart.quantity + "개 · 총 " + won.format(finalAmount) + "원(" + (isFree ? "무료배송" : "배송비 3,000원 포함") + ") 주문을 진행합니다.";
      if (checkoutStatus) {
        checkoutStatus.hidden = false;
        checkoutStatus.textContent = msg;
      }
      notify(msg);
      return;
    }

    const addBtn = event.target.closest("[data-store-add]");
    if (!addBtn || addBtn.disabled) return;
    const productId = addBtn.dataset.storeAdd;
    if (!products.has(productId)) return;
    const unresolved = awaitingRetry(productId);
    if (unresolved.length) void submit(unresolved, "retry");
    else void submit([newEvent(productId)], "add");
  });

  if (sheetOpenBtn) sheetOpenBtn.addEventListener("click", toggleCartSheet);
  if (sheetCloseBtn) sheetCloseBtn.addEventListener("click", closeCartSheet);
  if (sheetBackdrop) sheetBackdrop.addEventListener("click", closeCartSheet);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && cartSheet?.classList.contains("is-sheet-open")) {
      closeCartSheet();
    }
  });

  if (cartItems) {
    cartItems.addEventListener("click", handleLineClick);
    cartItems.addEventListener("change", handleQtyChange);
  }
  if (detailState) {
    detailState.addEventListener("click", handleLineClick);
    detailState.addEventListener("change", handleQtyChange);
  }

  const clearBtn = byId("store-clear-cart");
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      if (!currentCart || !currentCart.items.length) return;
      void mutateCart("/api/cart/clear/", {}, "장바구니를 비웠습니다.");
    });
  }

  const retryPendingBtn = byId("store-retry-pending");
  if (retryPendingBtn) retryPendingBtn.addEventListener("click", () => void submit(awaitingRetry(), "retry"));

  const reloadCartBtn = byId("store-reload-cart");
  if (reloadCartBtn) reloadCartBtn.addEventListener("click", () => void refreshCart(true, true));

  subscribeSync((msg) => {
    if (msg.type === "cart:action") {
      void refreshCart(false, true);
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") void refreshCart();
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) void refreshCart();
  });

  restoreRequests();
  setUpCatalog();
  if (currentCart) acceptCart(currentCart, { force: true });
  renderControls();
  void refreshCart();
})();
