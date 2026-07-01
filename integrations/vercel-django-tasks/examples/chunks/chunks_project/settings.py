from __future__ import annotations

SECRET_KEY = "vercel-django-tasks-example-chunks"  # noqa: S105
DEBUG = False
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = ["chunks"]
MIDDLEWARE: list[str] = []
ROOT_URLCONF = "chunks_project.urls"
ASGI_APPLICATION = "chunks_project.asgi.application"
WSGI_APPLICATION = "chunks_project.wsgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
