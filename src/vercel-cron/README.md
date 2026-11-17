# Cron

`vercel.cron` registers Python functions as cron jobs. Registered functions must
be module-level functions so they can be resolved as `module:function`.

## Function Decorator

```python
from vercel.cron import cron


@cron("0 9 * * 1")
def weekly_report() -> None:
    ...
```

## CronTab

```python
from vercel.cron import CronSchedule, CronTab

crons = CronTab()


@crons.register(CronSchedule(minute=0, hour=9, day_of_week=1))
def weekly_report() -> None:
    ...


jobs = crons.get_crons()
```

`CronSchedule.from_str("0 9 * * 1")` parses five-field cron strings.
