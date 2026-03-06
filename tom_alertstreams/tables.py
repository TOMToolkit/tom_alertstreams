from __future__ import annotations

import logging
from typing import Any

import django_filters
import django_tables2 as tables
from django import forms
from django.conf import settings
from django.utils.html import format_html
from django.utils.module_loading import import_string

from tom_alertstreams.models import Alert
from tom_common.htmx_table import HTMXTable, HTMXTableFilterSet

logger = logging.getLogger(__name__)


class AlertTable(HTMXTable):
    """HTMX-driven table of recent alerts from all configured alert streams.

    Receives url_map at construction time so that render_alert_id() can construct
    archive links on the fly, without storing URLs in the model. Streams with no
    ARCHIVE_URL_TEMPLATE (GCN, Hopskotch, stubs) display alert_id as plain text.
    """

    def __init__(self, *args: Any, url_map: dict[str, str | None] | None = None, **kwargs: Any) -> None:
        # Store url_map before calling super() so render_alert_id() can access it during rendering.
        self.url_map = url_map or {}
        super().__init__(*args, **kwargs)

    #
    # Custom field renderers
    #

    stream_name = tables.Column(verbose_name='Stream')  # sets the column header value

    # render_FIELDNAME() methods are called automatically when present
    def render_timestamp(self, value: Any) -> str:
        """Render timestamp in unambiguous UTC 24-hour format.

        The result looks like this: 2026-03-05 18:51:30 UTC
        """
        return value.strftime('%Y-%m-%d %H:%M:%S UTC')

    def render_alert_id(self, record: Alert, value: str) -> str:
        """Render alert_id as a hyperlink to the stream's archive if a URL template exists."""
        template = self.url_map.get(record.stream_name)
        if template:
            url = template.format(alert_id=record.alert_id, object_id=record.object_id or '')
            return format_html('<a href="{}">{}</a>', url, value)
        return value

    class Meta(HTMXTable.Meta):
        model = Alert
        fields = ['selection', 'alert_id', 'stream_name', 'topic', 'timestamp', 'object_id', 'ra', 'dec', 'magnitude']


def _get_stream_name_choices() -> list[tuple[str, str]]:
    """Build dropdown choices from the active streams in settings.ALERT_STREAMS.

    Called at form-render time (not import time) so changes to settings take
    effect without restarting the process. Returns a list of (value, label)
    tuples using each stream's STREAM_NAME. Streams that fail to import are
    silently skipped so a misconfigured entry doesn't break the filter form.
    """
    choices = []
    for stream_config in getattr(settings, 'ALERT_STREAMS', []):
        if not stream_config.get('ACTIVE', True):
            continue
        try:
            klass = import_string(stream_config['NAME'])
            name = klass.STREAM_NAME
            choices.append((name, name))
        except (ImportError, AttributeError, KeyError) as exc:
            logger.warning('_get_stream_name_choices: skipping stream %s: %s', stream_config.get('NAME'), exc)
    return choices


class AlertFilterSet(HTMXTableFilterSet):
    """FilterSet for the Recent Alerts table.

    Provides a 'query' full-text search (inherited from HTMXTableFilterSet) plus
    the fields defined here, which appear in the Advanced> expansion of the form.
    """
    # these are the fields that appear in the Advanced> expansion
    stream_name = django_filters.ChoiceFilter(
        field_name='stream_name',
        label='Stream',
        empty_label='All streams',
        choices=_get_stream_name_choices,
        widget=forms.Select(attrs={
            'hx-get': '',           # empty string: GET goes to the current page URL
            'hx-trigger': 'change',
            'hx-target': 'div.table-container',
            'hx-swap': 'innerHTML',
            'hx-indicator': '.progress',
            'hx-include': 'closest form',
        }),
    )

    class Meta:
        model = Alert
        fields = ['stream_name']
