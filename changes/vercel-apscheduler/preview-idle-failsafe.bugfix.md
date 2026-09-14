An opted-in preview whose driver document was evicted could lose its idle
deadline: the next wake adopted the chain from the message and kept it
running with no traffic requirement until the deployment was deleted. Start
and wake payloads now carry an `idle_bounded` marker, and a claim refuses to
adopt an idle-bounded chain into a document without a deadline, so an
evicted preview stops early and the next real request re-activates it. The
driver also logs whenever it rebuilds state from a message, making cache
eviction observable.
