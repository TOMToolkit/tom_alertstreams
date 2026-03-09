from __future__ import annotations

from typing import Any

from django.db.models import Max
from django.utils import timezone
from django_filters.views import FilterView

from tom_alertstreams.alertstreams.alertstream import get_alert_stream_classes
from tom_alertstreams.models import Alert
from tom_alertstreams.tables import (
    AlertFilterSet, AlertStreamPresenter, AlertTable, STREAM_PRESENTERS,
)
from tom_common.htmx_table import HTMXTableViewMixin


def _build_presenter_map() -> dict[str, AlertStreamPresenter]:
    """Build a presenter instance for each configured active alert stream.

    Looks up each stream's STREAM_NAME in the STREAM_PRESENTERS registry.
    Streams not in the registry get the default AlertStreamPresenter (no URLs).
    """
    return {
        klass.STREAM_NAME: STREAM_PRESENTERS.get(klass.STREAM_NAME, AlertStreamPresenter)()
        for klass in get_alert_stream_classes()
    }


def _build_stream_status() -> list[dict[str, Any]]:
    """Build per-stream "last seen" status for the dashboard.

    Returns a list of dicts with keys: stream_name, latest_timestamp, now.
    Includes all configured active streams — streams with no alerts in the
    database appear with latest_timestamp=None.

    One aggregate DB query (covered by the (stream_name, timestamp) index).
    """
    # Latest alert timestamp per stream, in one query
    latest_by_stream: dict[str, Any] = {
        row['stream_name']: row['latest']
        for row in Alert.objects.values('stream_name').annotate(latest=Max('timestamp'))
    }

    # Ordered list of configured stream names
    configured_streams = [klass.STREAM_NAME for klass in get_alert_stream_classes()]

    # Single now value so timesince is consistent across all badges
    now = timezone.now()
    return [
        {
            'stream_name': name,
            'latest_timestamp': latest_by_stream.get(name),
            'now': now,
        }
        for name in configured_streams
    ]


class RecentAlertsView(HTMXTableViewMixin, FilterView):
    """Display the most recent alerts from all configured alert streams.

    No login is required — the Recent Alerts page is intentionally public so that
    demo visitors and potential TOM developers can browse it without an account.

    Alert and object links (e.g. to ANTARES loci, ALeRCE objects) are built on the
    fly by AlertStreamPresenter subclasses (registered in tables.STREAM_PRESENTERS),
    so no URLs need to be stored in the database.
    """
    template_name = 'tom_alertstreams/recent_alerts.html'
    model = Alert
    table_class = AlertTable
    filterset_class = AlertFilterSet
    paginate_by = 20

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Add stream status data for the dashboard.

        Runs on every request (both full page and HTMX partial) so the OOB
        swap in the custom partial template can refresh the dashboard badges.
        """
        context = super().get_context_data(**kwargs)
        context['stream_status'] = _build_stream_status()
        return context

    def get_table_kwargs(self) -> dict[str, Any]:
        """Inject the presenter map into the AlertTable constructor.

        Each configured AlertStream is paired with an AlertStreamPresenter
        (looked up by STREAM_NAME in the STREAM_PRESENTERS registry). The
        presenter handles URL construction — the table just calls
        presenter.alert_url() / presenter.object_url() and renders the result.

        This method is implemented in django-tables2.SingleTableMixin,
        which HTMXTableViewMixin inherits from. It's called like this:

        get_context_data()          # SingleTableMixin (django-tables2)
          └── get_table(**self.get_table_kwargs())
                ├── get_table_kwargs()    # returns {} by default; we override
                └── get_table(presenter_map=…)
        """
        kwargs = super().get_table_kwargs()
        kwargs['presenter_map'] = _build_presenter_map()
        return kwargs
