The managed Redis backend was removed. The integration now always runs on its
managed job store (Vercel Runtime Cache); a configured default `RedisJobStore`
is rejected at import, `VERCEL_APSCHEDULER_BACKEND` accepts only `cache`, and
the `redis` dependency is gone. The scheduler's durable identity now always
derives from the builder-assigned subscriber id (previously the Redis
`jobs_key`); the `scheduler_id` option still pins an identity explicitly.
