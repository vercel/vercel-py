SECRET_KEY = "vercel-django-tasks-tests"
USE_TZ = True
INSTALLED_APPS: list[str] = []
TASKS = {
    "default": {
        "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "result_namespace": "django.results",
            "result_ttl_seconds": 120,
        },
    }
}
