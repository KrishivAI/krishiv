// Route tree, mirroring the platform console's conventions: authenticated
// routes hang off `appRoute`, which redirects to /login preserving the
// destination. Served under /console/ (router basepath matches Vite base).

import {
  Outlet,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";

import { session } from "./auth/session";
import { Shell } from "./components/Shell";
import { DashboardPage } from "./pages/Dashboard";
import { HistoryPage } from "./pages/History";
import { ExecutorsPage } from "./pages/Executors";
import { JobDetailPage } from "./pages/JobDetail";
import { JobsPage } from "./pages/Jobs";
import { LoginPage } from "./pages/Login";
import { SqlPage } from "./pages/Sql";
import { StreamingDetailPage } from "./pages/StreamingDetail";
import { StreamingPage } from "./pages/Streaming";

const rootRoute = createRootRoute({ component: () => <Outlet /> });

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: LoginPage,
});

/** Layout for everything behind the stored bearer. */
const appRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "app",
  beforeLoad: () => {
    if (!session.isConfigured()) {
      throw redirect({ to: "/login" });
    }
  },
  component: Shell,
});

const indexRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/",
  component: DashboardPage,
});
const jobsRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/jobs",
  component: JobsPage,
});
const jobDetailRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/jobs/$jobId",
  component: JobDetailPage,
});
const streamingRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/streaming",
  component: StreamingPage,
});
const streamingDetailRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/streaming/$jobId",
  component: StreamingDetailPage,
});
const executorsRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/executors",
  component: ExecutorsPage,
});
const historyRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/history",
  component: HistoryPage,
});
const sqlRoute = createRoute({
  getParentRoute: () => appRoute,
  path: "/sql",
  component: SqlPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  appRoute.addChildren([
    indexRoute,
    jobsRoute,
    jobDetailRoute,
    streamingRoute,
    streamingDetailRoute,
    executorsRoute,
    historyRoute,
    sqlRoute,
  ]),
]);

export const router = createRouter({ routeTree, basepath: "/console" });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
