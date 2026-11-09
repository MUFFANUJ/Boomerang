import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useLiveSelector, useLiveStore } from "../lib/live";
import { useTenant } from "../lib/tenant";
import { useNow } from "../lib/hooks";
import { LogoMark } from "./Logo";
import {
  IconChaos,
  IconEndpoints,
  IconEvents,
  IconFlow,
  IconInfra,
  IconOverview,
  IconSettings,
  IconTraffic,
} from "./Icons";

const NAV = [
  { to: "/", label: "Overview", icon: IconOverview, end: true },
  { to: "/flow", label: "Live Event Flow", icon: IconFlow },
  { to: "/traffic", label: "Traffic Control", icon: IconTraffic },
  { to: "/chaos", label: "Chaos Playground", icon: IconChaos },
  { section: "Configure" },
  { to: "/endpoints", label: "Endpoints", icon: IconEndpoints },
  { to: "/events", label: "Event Explorer", icon: IconEvents },
  { to: "/infra", label: "Infrastructure", icon: IconInfra },
  { to: "/settings", label: "Settings", icon: IconSettings },
] as const;

const TITLES: [RegExp, string][] = [
  [/^\/$/, "Overview"],
  [/^\/flow/, "Live Event Flow"],
  [/^\/traffic/, "Traffic Control"],
  [/^\/chaos/, "Chaos Playground"],
  [/^\/endpoints/, "Endpoints"],
  [/^\/events/, "Event Explorer"],
  [/^\/infra/, "Infrastructure"],
  [/^\/settings/, "Settings"],
];

function ConnectionIndicator() {
  const status = useLiveSelector((s) => s.status);
  const reconnectAt = useLiveSelector((s) => s.reconnectAt);
  const store = useLiveStore();
  const now = useNow(status === "closed" ? 500 : 5000);
  const label =
    status === "open"
      ? "Live"
      : status === "connecting"
        ? "Connecting…"
        : `Offline · retry in ${Math.max(0, Math.ceil(((reconnectAt ?? now) - now) / 1000))}s`;
  return (
    <button
      type="button"
      className={`conn conn-${status}`}
      onClick={() => store.reconnectNow()}
      title={status === "closed" ? "WebSocket disconnected. Click to reconnect now." : "WebSocket /ws"}
      style={{ cursor: status === "closed" ? "pointer" : "default" }}
    >
      <span className="conn-dot" />
      {label}
    </button>
  );
}

function TenantSwitcher() {
  const { tenantId, tenants, setTenantId, loading } = useTenant();
  const known = tenants.some((t) => t.id === tenantId);
  return (
    <label className="tenant-switch">
      <span className="muted" style={{ fontSize: 12 }}>
        Tenant
      </span>
      <select
        className="select"
        value={tenantId}
        onChange={(e) => setTenantId(e.target.value)}
        aria-label="Tenant"
      >
        {!known && <option value={tenantId}>{loading ? "Loading…" : tenantId}</option>}
        {tenants.map((t) => (
          <option key={t.id} value={t.id}>
            {t.name} ({t.id})
          </option>
        ))}
      </select>
    </label>
  );
}

export function Layout() {
  const { pathname } = useLocation();
  const title = TITLES.find(([re]) => re.test(pathname))?.[1] ?? "Boomerang";
  return (
    <div className="shell">
      <aside className="sidebar">
        <NavLink to="/" className="brand">
          <LogoMark />
          <div className="brand-text">
            <div className="brand-name">Boomerang</div>
            <div className="brand-sub">Webhook delivery</div>
          </div>
        </NavLink>
        <nav className="nav">
          {NAV.map((n, i) =>
            "section" in n ? (
              <div className="nav-section" key={i}>
                {n.section}
              </div>
            ) : (
              <NavLink key={n.to} to={n.to} end={"end" in n ? n.end : false} title={n.label}>
                <n.icon />
                <span>{n.label}</span>
              </NavLink>
            ),
          )}
        </nav>
        <div className="sidebar-foot">At-least-once delivery · Kafka · Go workers</div>
      </aside>
      <div className="main">
        <header className="topbar">
          <div className="topbar-title">{title}</div>
          <div className="topbar-spacer" />
          <TenantSwitcher />
          <ConnectionIndicator />
        </header>
        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
