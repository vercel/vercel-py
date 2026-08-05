Add Redis-backed APScheduler subscribers for Vercel Queues. The integration
patches `scheduler.start()`, `scheduler.pause()`, and `scheduler.resume()` with
durable, deployment-scoped lifecycle transitions and atomic single-chain
fencing. Paused occurrences are skipped on resume, and interrupted successor
publication is repaired on retry. Production schedules activate on the first
request, and opted-in previews stop after a durable idle timeout. Jobs that
do not choose a `misfire_grace_time` run their occurrences whenever the wake
arrives: the stock one-second grace assumes in-process wakeup precision that
queue delivery cannot meet.
