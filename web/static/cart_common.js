"use strict";

(() => {
  const SYNC_KEY = "deskroom.cart.sync.v1";
  const CHANNEL_NAME = "deskroom.cart.sync.v1";
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const won = new Intl.NumberFormat("ko-KR");

  function randomId() {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-");
  }

  function newEvent(productId = "") {
    return { product_id: productId, event_id: randomId() };
  }

  function getCsrfToken() {
    const input = document.querySelector("[name=csrfmiddlewaretoken]");
    if (input && input.value) return input.value;
    const match = document.cookie.match(/(?:^|;\s*)local_cart_csrf=([^;]+)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  async function requestJson(url, options = {}) {
    const abort = new AbortController();
    const timeout = window.setTimeout(() => abort.abort(), 12000);
    try {
      const response = await fetch(url, {
        credentials: "same-origin",
        cache: "no-store",
        ...options,
        signal: abort.signal,
      });
      if (!response.ok) {
        const body = await response.json().catch(() => null);
        const error = new Error(body && typeof body.error === "string" ? body.error : "요청을 처리할 수 없습니다.");
        error.status = response.status;
        error.fault = body?.fault;
        error.eventId = body?.event_id;
        throw error;
      }
      return await response.json();
    } finally {
      window.clearTimeout(timeout);
    }
  }

  let sharedChannel = null;
  function getChannel() {
    if (!sharedChannel && typeof BroadcastChannel === "function") {
      try {
        sharedChannel = new BroadcastChannel(CHANNEL_NAME);
      } catch {
        sharedChannel = null;
      }
    }
    return sharedChannel;
  }

  const seenMessages = new Set();
  function rememberSeen(msgId) {
    if (!msgId) return true;
    if (seenMessages.has(msgId)) return false;
    seenMessages.add(msgId);
    if (seenMessages.size > 60) {
      seenMessages.delete(seenMessages.values().next().value);
    }
    return true;
  }

  function publishSync(payload) {
    const message = { ...payload, msgId: randomId(), ts: Date.now() };
    const channel = getChannel();
    if (channel) {
      try {
        channel.postMessage(message);
      } catch {
        // Fallback storage event below still notifies peer windows.
      }
    }
    try {
      localStorage.setItem(SYNC_KEY, JSON.stringify(message));
    } catch {
      // Private or restricted storage does not block the active window.
    }
    return message;
  }

  function subscribeSync(handler) {
    const handle = (raw) => {
      if (!raw || typeof raw !== "object" || !raw.type) return;
      if (!rememberSeen(raw.msgId)) return;
      handler(raw);
    };

    const channel = getChannel();
    const onChannel = (event) => handle(event.data);
    if (channel) channel.addEventListener("message", onChannel);

    const onStorage = (event) => {
      if (event.key !== SYNC_KEY || !event.newValue) return;
      try {
        handle(JSON.parse(event.newValue));
      } catch {
        // Ignore malformed payloads from external storage edits.
      }
    };
    window.addEventListener("storage", onStorage);

    return () => {
      if (channel) channel.removeEventListener("message", onChannel);
      window.removeEventListener("storage", onStorage);
    };
  }

  window.Deskroom = {
    SYNC_KEY,
    uuidPattern,
    won,
    newEvent,
    getCsrfToken,
    requestJson,
    publishSync,
    subscribeSync,
  };
})();
