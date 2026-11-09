// Current tenant (defaults to ten_shopsim) + the tenant list for the switcher.
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { api } from "../api/client";
import type { Tenant } from "../api/types";

export const DEFAULT_TENANT = "ten_shopsim";
const STORAGE_KEY = "boomerang.tenant";

interface TenantCtx {
  tenantId: string;
  tenant: Tenant | null;
  tenants: Tenant[];
  loading: boolean;
  error: unknown;
  setTenantId: (id: string) => void;
  refresh: () => Promise<void>;
  /** Insert or replace a tenant in the cached list (after create/patch). */
  upsert: (t: Tenant) => void;
}

const Ctx = createContext<TenantCtx | null>(null);

function readStored(): string {
  try {
    return window.localStorage.getItem(STORAGE_KEY) || DEFAULT_TENANT;
  } catch {
    return DEFAULT_TENANT;
  }
}

export function TenantProvider({ children }: { children: ReactNode }) {
  const [tenantId, setTenantIdState] = useState<string>(readStored);
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);

  const refresh = useCallback(async () => {
    try {
      const list = await api.listTenants();
      setTenants(list);
      setError(null);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // If the stored tenant no longer exists, fall back to the demo tenant.
  useEffect(() => {
    if (tenants.length && !tenants.some((t) => t.id === tenantId)) {
      const fallback = tenants.find((t) => t.id === DEFAULT_TENANT)?.id ?? tenants[0].id;
      setTenantIdState(fallback);
    }
  }, [tenants, tenantId]);

  const setTenantId = useCallback((id: string) => {
    setTenantIdState(id);
    try {
      window.localStorage.setItem(STORAGE_KEY, id);
    } catch {
      /* ignore */
    }
  }, []);

  const upsert = useCallback((t: Tenant) => {
    setTenants((prev) => {
      const i = prev.findIndex((x) => x.id === t.id);
      if (i === -1) return [...prev, t];
      const next = prev.slice();
      next[i] = t;
      return next;
    });
  }, []);

  const value = useMemo<TenantCtx>(
    () => ({
      tenantId,
      tenant: tenants.find((t) => t.id === tenantId) ?? null,
      tenants,
      loading,
      error,
      setTenantId,
      refresh,
      upsert,
    }),
    [tenantId, tenants, loading, error, setTenantId, refresh, upsert],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useTenant(): TenantCtx {
  const c = useContext(Ctx);
  if (!c) throw new Error("useTenant must be used inside <TenantProvider>");
  return c;
}
