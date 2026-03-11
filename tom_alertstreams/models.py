from __future__ import annotations

from typing import Any, ClassVar

from django.conf import settings
from django.db import models


class FIFOQueueMixin(models.Model):
    """Mixin that enforces a per-partition maximum row count.

    Basically, this limits the size that a Model's table can reach by
    turning it into a First-In-First-Out (FIFO) queue.
    
    This is implemented by extending the `save()` method to save a model
    instance (as per normal), then check the table size and delete records
    (oldest first) over the FIFO_MAX limit.

    The wrinkle is the per-partion part: The FIFO_PARTIION_FIELD is a field
    in the model that divides (i.e. "partitions") the table according to the
    value of the field. What that means is that there can be FIFO_MAX records
    that have a common value in the FIFO_PARTIION_FIELD. So, for example, if the
    FIFO_PARTITION_FIELD is `stream_name`, then there can be FIFO_MAX records
    with `stream_name` "alerce" and FIFO_MAX records with `stream_name` "fink",
    etc. So, there can be FIFO_MAX records for each distinct value of the
    FIFO_PARTIION_FIELD. The FIFO_PARTION_FIELD value of the instance being
    saved specifies the partition whose size is checked, post-save().

    To summarize, after every save(), rows beyond FIFO_MAX are deleted
    (oldest first) within the same partition as the newly-saved instance.
    'Partition' means all rows sharing the same value of FIFO_PARTITION_FIELD
    — e.g., all alerts from the same stream. If FIFO_PARTITION_FIELD is None,
    then the FIFO_MAX limit applies to the entire table.

    Subclasses set FIFO_MAX and FIFO_PARTITION_FIELD as class variables.
    """
    FIFO_MAX: ClassVar[int] = 10
    FIFO_PARTITION_FIELD: ClassVar[str | None] = None

    class Meta:
        abstract = True

    def save(self, *args: Any, **kwargs: Any) -> None:
        super().save(*args, **kwargs)  # save, then trim: Ensures the newly-saved
        self._enforce_fifo_limit()     # row is counted against the FIFO_MAX limit.

    def _enforce_fifo_limit(self) -> None:
        """Delete rows beyond FIFO_MAX, oldest first, within this instance's partition.

        Uses list() to materialise PKs before the DELETE to avoid a SQLite restriction
        that forbids DELETE from a table referenced in the same statement's subquery.
        """
        qs = self.__class__.objects.all()
        if self.FIFO_PARTITION_FIELD is not None:
            # Scope the FIFO_MAX limit to rows in the same partition as this instance.
            partition_value = getattr(self, self.FIFO_PARTITION_FIELD)
            qs = qs.filter(**{self.FIFO_PARTITION_FIELD: partition_value})
        # Materialise PKs to avoid a subquery-in-DELETE issue on SQLite.
        excess_pks = list(
            qs.order_by('-timestamp').values_list('pk', flat=True)[self.FIFO_MAX:]
        )
        if excess_pks:
            self.__class__.objects.filter(pk__in=excess_pks).delete()


class Alert(FIFOQueueMixin):
    """A normalized alert received from an alert stream, stored for recent display.

    This class is designed specifically for a Recent Alerts demonstration page.

    Because of the FIFOQueueMixin, rows are automatically pruned to
    ALERTSTREAMS_RECENT_COUNT per stream_name. The raw_payload JSONField preserves
    the full original alert for handlers or views that need stream-specific
    fields not captured here.
    """
    # set the mixin class variables
    FIFO_MAX: ClassVar[int] = getattr(settings, 'ALERTSTREAMS_RECENT_COUNT', 10)
    FIFO_PARTITION_FIELD: ClassVar[str | None] = 'stream_name'

    stream_name = models.CharField(max_length=100, db_index=True)
    topic = models.CharField(max_length=200)
    timestamp = models.DateTimeField(db_index=True)
    alert_id = models.CharField(max_length=200)
    object_id = models.CharField(max_length=200, blank=True, null=True)
    ra = models.FloatField(null=True)
    dec = models.FloatField(null=True)
    magnitude = models.FloatField(null=True)
    flux = models.FloatField(null=True)
    raw_payload = models.JSONField(default=dict)

    class Meta(FIFOQueueMixin.Meta):  # this is the way you subclass the internal Meta class
        abstract = False  # override for the concrete model (abstract is True in the super)
        ordering = ['-timestamp']
        indexes = [models.Index(fields=['stream_name', 'timestamp'])]

    def __str__(self) -> str:
        return f'Alert {self.alert_id} from {self.stream_name} at {self.timestamp}'
