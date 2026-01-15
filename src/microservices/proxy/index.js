const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");

const PORT = Number(process.env.PORT || 8000);

const MONOLITH_URL = process.env.MONOLITH_URL || "http://monolith:8080";
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || "http://movies-service:8081";
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || "http://events-service:8082";

const GRADUAL_MIGRATION = (process.env.GRADUAL_MIGRATION || "true").toLowerCase() === "true";
const MOVIES_MIGRATION_PERCENT = Math.max(
  0,
  Math.min(100, Number(process.env.MOVIES_MIGRATION_PERCENT || 0))
);

function shouldRouteToMoviesService() {
  if (!GRADUAL_MIGRATION) return true;
  return Math.random() * 100 < MOVIES_MIGRATION_PERCENT;
}

const app = express();

function createServiceProxy(target) {
  return createProxyMiddleware({
    target,
    changeOrigin: true,
    xfwd: true,
    logLevel: "warn",
  });
}

function preserveOriginalUrl(req) {
  req.url = req.originalUrl;
}

app.use("/api/movies/health", (req, res, next) => {
  preserveOriginalUrl(req);
  return createServiceProxy(MOVIES_SERVICE_URL)(req, res, next);
});

app.use("/api/movies", (req, res, next) => {
  preserveOriginalUrl(req);
  const target = shouldRouteToMoviesService() ? MOVIES_SERVICE_URL : MONOLITH_URL;
  return createServiceProxy(target)(req, res, next);
});

app.use("/api/events", (req, res, next) => {
  preserveOriginalUrl(req);
  return createServiceProxy(EVENTS_SERVICE_URL)(req, res, next);
});

app.use(
  "/",
  createServiceProxy(MONOLITH_URL)
);

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    JSON.stringify(
      {
        msg: "proxy-service started",
        PORT,
        MONOLITH_URL,
        MOVIES_SERVICE_URL,
        EVENTS_SERVICE_URL,
        GRADUAL_MIGRATION,
        MOVIES_MIGRATION_PERCENT,
      },
      null,
      2
    )
  );
});
