Add Redis-backed APScheduler subscribers for Vercel Queues. The integration
patches `scheduler.start()`, `scheduler.pause()`, and `scheduler.resume()` with
durable, deployment-scoped lifecycle transitions and atomic single-chain
fencing. Paused occurrences are skipped on resume, and interrupted successor
publication is repaired on retry.
