// Minimal toast notifications for action feedback.
import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from "react";
import { errorMessage } from "../api/client";
import { IconAlert, IconCheck, IconInfo } from "../components/Icons";

type Tone = "good" | "critical" | "info";
interface Toast {
  id: number;
  tone: Tone;
  text: string;
}

interface ToastApi {
  success: (text: string) => void;
  error: (errOrText: unknown) => void;
  info: (text: string) => void;
}

const Ctx = createContext<ToastApi | null>(null);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const idRef = useRef(0);

  const push = useCallback((tone: Tone, text: string) => {
    const id = ++idRef.current;
    setToasts((t) => [...t.slice(-3), { id, tone, text }]);
    window.setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), tone === "critical" ? 7000 : 4000);
  }, []);

  const api = useMemo<ToastApi>(
    () => ({
      success: (text) => push("good", text),
      error: (e) => push("critical", typeof e === "string" ? e : errorMessage(e)),
      info: (text) => push("info", text),
    }),
    [push],
  );

  return (
    <Ctx.Provider value={api}>
      {children}
      <div className="toasts" role="status" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`toast toast-${t.tone}`}>
            {t.tone === "good" ? <IconCheck /> : t.tone === "critical" ? <IconAlert /> : <IconInfo />}
            <span>{t.text}</span>
          </div>
        ))}
      </div>
    </Ctx.Provider>
  );
}

export function useToast(): ToastApi {
  const c = useContext(Ctx);
  if (!c) throw new Error("useToast must be used inside <ToastProvider>");
  return c;
}
