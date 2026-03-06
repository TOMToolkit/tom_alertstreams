from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string
from django_filters.views import FilterView

from tom_alertstreams.models import Alert
from tom_alertstreams.tables import AlertFilterSet, AlertTable
from tom_common.htmx_table import HTMXTableViewMixin

logger = logging.getLogger(__name__)


def _build_archive_url_map() -> dict[str, str | None]:
    """Build {stream_name → archive_url_template} from ALERT_STREAMS settings.

    Reads ALERT_STREAMS and imports each active stream class by dotted path to
    access its STREAM_NAME and ARCHIVE_URL_TEMPLATE class variables. Does NOT
    instantiate the streams — no network connections are made. Returns an empty
    dict if ALERT_STREAMS is not configured.
    """
    url_map: dict[str, str | None] = {}
    for stream_config in getattr(settings, 'ALERT_STREAMS', []):
        if not stream_config.get('ACTIVE', True):
            continue
        try:
            klass = import_string(stream_config['NAME'])
            url_map[klass.STREAM_NAME] = klass.ARCHIVE_URL_TEMPLATE
        except (ImportError, AttributeError, KeyError) as exc:
            logger.warning(f'_build_archive_url_map: could not read stream class {stream_config.get("NAME")}: {exc}')
    return url_map


class RecentAlertsView(HTMXTableViewMixin, FilterView):
    """Display the most recent alerts from all configured alert streams.

    No login is required — the Recent Alerts page is intentionally public so that
    demo visitors and potential TOM developers can browse it without an account.

    Archive URL links (e.g. to ANTARES, ALeRCE) are built on the fly from each
    stream's ARCHIVE_URL_TEMPLATE class variable, so no URL needs to be stored
    in the database.
    """
    template_name = 'tom_alertstreams/recent_alerts.html'
    model = Alert
    table_class = AlertTable
    filterset_class = AlertFilterSet
    paginate_by = 20

    def get_table_kwargs(self) -> dict[str, Any]:
        """Inject the archive url_map into the AlertTable constructor.

        The "archive url map" is used to create links that appear in the
        Recent Alerts table. The link is to the alert at the alert brokers site.

        The map is built above using the class property set on the AlertStream
        subclasses.

        This method is implemented in django-tables2.SingleTableMixin,
        which HTMXTableViewMixin inherits from. It's called like this:

        get_context_data()          # SingleTableMixin (django-tables2)
          └── get_table(**self.get_table_kwargs())
                ├── get_table_kwargs()    # returns {} by default; we override to add url_map
                └── get_table(url_map=…)  # instantiates AlertTable(data=…, url_map=…)
        """
        kwargs = super().get_table_kwargs()
        kwargs['url_map'] = _build_archive_url_map()
        return kwargs
