More reliably fail runs whose replay diverges from the event log.

Runs will now fail even in the case where the main thread of execution
is not directly blocked on the suspension that is erroring.
