# Subscriber entrypoint ("worker:broker"). Importing tasks declares the
# Dramatiq actor's queues on the broker so the queue subscriber can execute
# them.
import dramatiq
import tasks  # noqa: F401

broker = dramatiq.get_broker()

__all__ = ["broker"]
