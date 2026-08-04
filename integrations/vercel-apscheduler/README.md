# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages, with
Redis as the durable job store and lifecycle coordinator.

This package currently ships the durable Redis driver: the atomic lifecycle
and wake-chain state machine that the APScheduler adapter builds on. The
runtime model is documented in [SCHEDULER.md](SCHEDULER.md).
