Require `celery>=5.3.1`; Celery 5.3.0 keeps closed apps alive, so their queue registrations were never released.
