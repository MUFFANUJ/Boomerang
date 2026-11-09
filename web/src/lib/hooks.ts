import { useCallback, useEffect, useRef, useState } from "react";
import { isAbort } from "../api/client";

export interface AsyncState<T> {
  data: T | undefined;
  error: unknown;
  loading: boolean;
  /** Re-run the loader. Keeps the previous data while loading (no flash). */
  reload: () => void;
  setData: (updater: T | ((prev: T | undefined) => T)) => void;
}

/**
 * Runs an async loader whenever deps change. Aborts in-flight requests on change/unmount.
 * Previous data is kept while reloading, so views can hold their frame.
 */
export function useAsync<T>(
  loader: (signal: AbortSignal) => Promise<T>,
  deps: unknown[],
  opts: { pollMs?: number; enabled?: boolean } = {},
): AsyncState<T> {
  const [data, setDataState] = useState<T | undefined>(undefined);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState<boolean>(opts.enabled !== false);
  const [nonce, setNonce] = useState(0);
  const loaderRef = useRef(loader);
  loaderRef.current = loader;
  const enabled = opts.enabled !== false;

  useEffect(() => {
    if (!enabled) {
      setLoading(false);
      return;
    }
    const ctrl = new AbortController();
    let timer: number | undefined;
    let cancelled = false;

    const run = async (isPoll: boolean) => {
      if (!isPoll) setLoading(true);
      try {
        const result = await loaderRef.current(ctrl.signal);
        if (cancelled) return;
        setDataState(result);
        setError(null);
      } catch (err) {
        if (cancelled || isAbort(err)) return;
        setError(err);
      } finally {
        if (!cancelled) {
          setLoading(false);
          if (opts.pollMs) timer = window.setTimeout(() => void run(true), opts.pollMs);
        }
      }
    };
    void run(false);
    return () => {
      cancelled = true;
      ctrl.abort();
      if (timer) window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce, enabled, opts.pollMs]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  const setData = useCallback((updater: T | ((prev: T | undefined) => T)) => {
    setDataState((prev) =>
      typeof updater === "function" ? (updater as (p: T | undefined) => T)(prev) : updater,
    );
  }, []);

  return { data, error, loading, reload, setData };
}

/** Debounced copy of a value. */
export function useDebounced<T>(value: T, ms: number): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = window.setTimeout(() => setV(value), ms);
    return () => window.clearTimeout(t);
  }, [value, ms]);
  return v;
}

/** Re-renders every `ms` and returns Date.now(). */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return now;
}

/** Tracks an element's content-box size. */
export function useElementSize<T extends HTMLElement>(): [
  (el: T | null) => void,
  { width: number; height: number },
] {
  const [size, setSize] = useState({ width: 0, height: 0 });
  const roRef = useRef<ResizeObserver | null>(null);
  const ref = useCallback((el: T | null) => {
    roRef.current?.disconnect();
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const r = entries[0]?.contentRect;
      if (r) setSize((s) => (s.width === r.width && s.height === r.height ? s : { width: r.width, height: r.height }));
    });
    ro.observe(el);
    roRef.current = ro;
  }, []);
  return [ref, size];
}

/** Runs a mutating action and tracks its busy state. */
export function useAction<A extends unknown[], R>(
  fn: (...args: A) => Promise<R>,
): [(...args: A) => Promise<R | undefined>, boolean] {
  const [busy, setBusy] = useState(false);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  const run = useCallback(async (...args: A) => {
    setBusy(true);
    try {
      return await fnRef.current(...args);
    } finally {
      setBusy(false);
    }
  }, []);
  return [run, busy];
}

/** localStorage-backed state; tolerates storage being unavailable. */
export function useStoredState<T>(key: string, initial: T): [T, (v: T) => void] {
  const [v, setV] = useState<T>(() => {
    try {
      const raw = window.localStorage.getItem(key);
      return raw === null ? initial : (JSON.parse(raw) as T);
    } catch {
      return initial;
    }
  });
  const set = useCallback(
    (next: T) => {
      setV(next);
      try {
        window.localStorage.setItem(key, JSON.stringify(next));
      } catch {
        /* storage unavailable */
      }
    },
    [key],
  );
  return [v, set];
}
