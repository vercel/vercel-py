from __future__ import annotations

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "chunks_project.settings")

application = get_wsgi_application()

from chunks import tasks  # noqa: E402

from vercel.integrations.django import install_vercel_django_task_integration  # noqa: E402

_ = tasks
install_vercel_django_task_integration()
