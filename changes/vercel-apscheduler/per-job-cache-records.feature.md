The managed job store now keeps one Runtime Cache record per declared job
instead of a single document holding every job. The declarations are the
index: reads enumerate the code-declared ids, and a record that is missing,
unreadable, or written for a different declared schedule is rebuilt from its
declaration at the point of use, with a log line making cache eviction
observable. Eviction and write races now cost at most one job's progress
instead of the whole population's, the per-item size limit no longer bounds
the job count, and takeover syncs to the new code's declarations lazily with
no reconciliation sweep or marker.
