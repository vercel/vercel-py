from __future__ import annotations

from django.urls import path

from . import views

urlpatterns = [
    path("send_chunks/", views.send_chunks, name="send_chunks"),
]
