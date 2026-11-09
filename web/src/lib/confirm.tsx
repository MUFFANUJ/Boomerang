// Promise-based confirmation dialog: `if (await confirm({...})) doIt()`.
import { createContext, useCallback, useContext, useState, type ReactNode } from "react";
import { Modal } from "../components/Modal";

interface ConfirmOptions {
  title: string;
  body?: ReactNode;
  confirmLabel?: string;
  danger?: boolean;
}

type ConfirmFn = (opts: ConfirmOptions) => Promise<boolean>;
const Ctx = createContext<ConfirmFn | null>(null);

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [pending, setPending] = useState<(ConfirmOptions & { resolve: (v: boolean) => void }) | null>(null);

  const confirm = useCallback<ConfirmFn>(
    (opts) => new Promise<boolean>((resolve) => setPending({ ...opts, resolve })),
    [],
  );

  const close = (v: boolean) => {
    pending?.resolve(v);
    setPending(null);
  };

  return (
    <Ctx.Provider value={confirm}>
      {children}
      {pending && (
        <Modal
          title={pending.title}
          onClose={() => close(false)}
          footer={
            <>
              <button className="btn" onClick={() => close(false)}>
                Cancel
              </button>
              <button
                className={pending.danger ? "btn btn-solid-danger" : "btn btn-primary"}
                onClick={() => close(true)}
                autoFocus
              >
                {pending.confirmLabel ?? "Confirm"}
              </button>
            </>
          }
        >
          {pending.body && <div className="secondary">{pending.body}</div>}
        </Modal>
      )}
    </Ctx.Provider>
  );
}

export function useConfirm(): ConfirmFn {
  const c = useContext(Ctx);
  if (!c) throw new Error("useConfirm must be used inside <ConfirmProvider>");
  return c;
}
