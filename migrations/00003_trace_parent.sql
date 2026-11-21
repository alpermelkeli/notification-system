-- The W3C traceparent of the request that created each notification (B-5, ADR-0009). The
-- dispatcher starts every dispatch span as its child, so the API request, each dispatch, each
-- delivery attempt and the provider call share one trace even across retries and restarts.
-- Empty when tracing was off or the request was not sampled.

-- +goose Up

-- A constant default only changes the catalog (PostgreSQL 11+); no partition is rewritten.
ALTER TABLE notifications ADD COLUMN trace_parent text NOT NULL DEFAULT '';
