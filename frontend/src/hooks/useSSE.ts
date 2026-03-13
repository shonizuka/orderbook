/**
 * Generic SSE hook.
 * Opens an EventSource to `url`, calls `onMessage` for each event.
 * Reconnects automatically on error (native EventSource behaviour).
 */
import { useEffect, useRef } from "react";

export function useSSE<T>(
  url: string,
  onMessage: (data: T) => void,
  enabled = true
): void {
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  useEffect(() => {
    if (!enabled) return;

    const es = new EventSource(url);

    es.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data) as T;
        onMessageRef.current(data);
      } catch {
        // Ignore malformed frames
      }
    };

    es.onerror = () => {
      // EventSource reconnects automatically — nothing to do here
    };

    return () => {
      es.close();
    };
  }, [url, enabled]);
}
