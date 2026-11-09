import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { LiveProvider } from "./lib/live";
import { TenantProvider } from "./lib/tenant";
import { ToastProvider } from "./lib/toast";
import { ConfirmProvider } from "./lib/confirm";
import { OverviewPage } from "./pages/Overview";
import { FlowPage } from "./pages/flow/FlowPage";
import { TrafficPage } from "./pages/Traffic";
import { ChaosPage } from "./pages/Chaos";
import { EndpointsPage } from "./pages/Endpoints";
import { EventsPage } from "./pages/Events";
import { InfraPage } from "./pages/Infrastructure";
import { SettingsPage } from "./pages/Settings";
import { EmptyState } from "./components/ui";

export function App() {
  return (
    <BrowserRouter>
      <ToastProvider>
        <ConfirmProvider>
          <TenantProvider>
            <LiveProvider>
              <Routes>
                <Route element={<Layout />}>
                  <Route index element={<OverviewPage />} />
                  <Route path="flow" element={<FlowPage />} />
                  <Route path="traffic" element={<TrafficPage />} />
                  <Route path="chaos" element={<ChaosPage />} />
                  <Route path="endpoints" element={<EndpointsPage />} />
                  <Route path="events" element={<EventsPage />} />
                  <Route path="events/:eventId" element={<EventsPage />} />
                  <Route path="infra" element={<InfraPage />} />
                  <Route path="settings" element={<SettingsPage />} />
                  <Route path="*" element={<EmptyState title="Page not found">Pick a page from the sidebar.</EmptyState>} />
                </Route>
              </Routes>
            </LiveProvider>
          </TenantProvider>
        </ConfirmProvider>
      </ToastProvider>
    </BrowserRouter>
  );
}
