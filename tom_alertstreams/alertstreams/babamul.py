from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from tom_alertstreams.alertstreams.alertstream import AlertStream, AlertStreamConfig, NormalizedAlert

logger = logging.getLogger(__name__)

# TODO: remove when stubs are replaced
# Mock data constants — chosen to be unmistakably non-astronomical:
# (0, 0) is not a real survey pointing; 99.0 is the astronomical sentinel for "no data".
_MOCK_RA = 0.0
_MOCK_DEC = 0.0
_MOCK_MAGNITUDE = 99.0


class BabamulConfig(AlertStreamConfig):
    """Pydantic configuration model for BabamulAlertStream (stub).

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    """
    # TODO: replace this stub with actual implementation
    pass


class BabamulAlertStream(AlertStream):
    """Stub Babamul AlertStream that generates obviously-fake mock alerts.
    """
    configuration_class = BabamulConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'babamul'
    ARCHIVE_URL_TEMPLATE: ClassVar[str | None] = None

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock Babamul alert dict to a NormalizedAlert.

        Args:
            raw_alert: Dict produced by listen(); contains mock field values.
            topic: Kafka topic the alert was consumed from.

        Returns:
            NormalizedAlert populated from the mock dict fields.
        """
        # TODO: replace this stub with actual implementation
        # super().normalized_alert is @abs.abstractmethod, so the stub needs an implementation
        normalized_alert = NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic or raw_alert.get('topic', ''),
            timestamp=datetime.fromisoformat(raw_alert['timestamp']),
            alert_id=raw_alert['alert_id'],
            object_id=raw_alert.get('object_id'),
            ra=raw_alert.get('ra'),
            dec=raw_alert.get('dec'),
            magnitude=raw_alert.get('magnitude'),
            raw_payload=raw_alert,
        )
        return normalized_alert

    def listen(self) -> None:
        """Generate mock Babamul alerts and dispatch to configured topic handlers.

        Loops indefinitely, emitting one mock alert per iteration with a random
        5–30 second delay. Topics are round-robined if multiple are configured.
        """
        # TODO: replace this stub with actual implementation
        counter = 0
        topics = list(self.config.TOPIC_HANDLERS.keys())

        # for this stub, generate mock alerts (rather than listen to the stream) endlessly
        while True:
            counter += 1
            topic = topics[counter % len(topics)]
            timestamp = datetime.now(timezone.utc)
            object_id = f'MOCK-{self.STREAM_NAME.upper()}-{counter:04d}'
            alert_id = f'MOCK-{timestamp.strftime("%Y%m%d%H%M%S")}'
            mock_alert: dict[str, Any] = {
                'alert_id': alert_id,
                'object_id': object_id,
                'topic': topic,
                'timestamp': timestamp.isoformat(),
                'ra': _MOCK_RA,
                'dec': _MOCK_DEC,
                'magnitude': _MOCK_MAGNITUDE,
                'mock': True,
            }
            logger.debug(f'BabamulAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(5.0, 30.0))
