import logging
from .alertstream import AlertStream
from antares_client.stream import StreamingClient

logger = logging.getLogger(__name__)


class AntaresAlertStream(AlertStream):
    """
    Wrapper for the ANTARES broker streaming client. See https://nsf-noirlab.gitlab.io/csdc/antares/client/.
    """
    required_keys = ['API_KEY', 'API_SECRET', 'TOPIC_HANDLERS']
    allowed_keys = ['API_KEY', 'API_SECRET', 'TOPIC_HANDLERS', 'GROUP', 'SSL_CA_LOCATION', 'ENABLE_AUTO_COMMIT']
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        logger.debug(f'AntaresAlertStream.__init__() kwargs: {kwargs}')
        optional_keys = set(self.allowed_keys) - set(self.required_keys)
        options = {key.lower(): kwargs[key] for key in optional_keys if key in kwargs}
        self.stream = StreamingClient(
            topics=self.topic_handlers.keys(),
            api_key=self.api_key,
            api_secret=self.api_secret,
            **options
        )

    def listen(self):
        for topic, locus in self.stream.iter():
            base_topic = topic.removeprefix(self.stream._TOPIC_PREFIX)
            logger.info(f"received {locus.locus_id} on {base_topic}")
            self.alert_handler[base_topic](locus)
