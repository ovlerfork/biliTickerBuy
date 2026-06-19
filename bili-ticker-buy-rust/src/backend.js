import { invoke as tauriInvoke } from "@tauri-apps/api/tauri";
import { getVersion as tauriGetVersion } from "@tauri-apps/api/app";
import { listen as tauriListen } from "@tauri-apps/api/event";
import {
    isPermissionGranted as tauriIsPermissionGranted,
    requestPermission as tauriRequestPermission,
    sendNotification as tauriSendNotification
} from "@tauri-apps/api/notification";

export const isWeb = import.meta.env.VITE_APP_TARGET === "web";

const listeners = new Map();
let eventCursor = 0;
let polling = false;

export async function invoke(cmd, args = {}) {
    if (!isWeb) return tauriInvoke(cmd, args);

    const res = await fetch("/api/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ cmd, args })
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "请求失败");
    return data.value;
}

export function getVersion() {
    return isWeb ? Promise.resolve("web") : tauriGetVersion();
}

export async function listen(event, handler) {
    if (!isWeb) return tauriListen(event, handler);

    const set = listeners.get(event) || new Set();
    set.add(handler);
    listeners.set(event, set);
    startPolling();
    return () => set.delete(handler);
}

export async function isPermissionGranted() {
    if (!isWeb) return tauriIsPermissionGranted();
    if (!("Notification" in window)) return false;
    return Notification.permission === "granted";
}

export async function requestPermission() {
    if (!isWeb) return tauriRequestPermission();
    if (!("Notification" in window)) return "denied";
    return Notification.requestPermission();
}

export function sendNotification(options) {
    if (!isWeb) return tauriSendNotification(options);
    if ("Notification" in window && Notification.permission === "granted") {
        new Notification(options.title, { body: options.body });
    }
}

function startPolling() {
    if (polling) return;
    polling = true;
    pollEvents();
}

async function pollEvents() {
    while (polling) {
        try {
            const res = await fetch(`/api/events?since=${eventCursor}`);
            const data = await res.json();
            eventCursor = data.next;
            for (const item of data.events || []) {
                for (const handler of listeners.get(item.event) || []) {
                    handler({ payload: item.payload });
                }
            }
        } catch (e) {
            console.error("event polling failed", e);
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
    }
}
