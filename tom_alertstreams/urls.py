from __future__ import annotations

from django.urls import path

from tom_alertstreams.views import RecentAlertsView

app_name = 'tom_alertstreams'

urlpatterns = [
    path('recent/', RecentAlertsView.as_view(), name='recent-alerts'),
]
