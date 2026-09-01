Celery tasks with an explicit hard `time_limit` are now sharded into
per-limit queue topics, each served by its own deployed function with a
matching `max_duration`. Tasks without an explicit limit stay on the
queue's base topic, whose function limit comes from the new `max_duration`
entry in `broker_transport_options` or the app's `task_time_limit` setting.
