"""Connect to pubnub."""

import datetime
import logging

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNReconnectionPolicy, PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

from yalexs.const import PUBNUB_TOKENS, Brand

_LOGGER = logging.getLogger(__name__)

class AugustPubNub(SubscribeCallback):
    def __init__(self, *args, **kwargs):
        """Initialize the AugustPubNub."""
        super().__init__(*args, **kwargs)
        self.connected = False
        self._device_channels = {}
        self._subscriptions = []

    def presence(self, pubnub, presence):
        _LOGGER.debug("Received new presence: %s", presence)

    def status(self, pubnub, status):
        if not pubnub:
            self.connected = False
            return

        _LOGGER.debug(
            "Received new status: category=%s error_data=%s error=%s status_code=%s operation=%s",
            status.category,
            status.error_data,
            status.error,
            status.status_code,
            status.operation,
        )

        if status.category in (
            PNStatusCategory.PNUnknownCategory,
            PNStatusCategory.PNUnexpectedDisconnectCategory,
            PNStatusCategory.PNNetworkIssuesCategory,
            PNStatusCategory.PNTimeoutCategory,
        ):
            self.connected = False
            pubnub.reconnect()

        elif status.category == PNStatusCategory.PNReconnectedCategory:
            self.connected = True
            now = datetime.datetime.utcnow()
            # Callback with an empty message to force a refresh
            for callback in self._subscriptions:
                for device_id in self._device_channels.values():
                    callback(device_id, now, {})

        elif status.category == PNStatusCategory.PNConnectedCategory:
            self.connected = True

    def message(self, pubnub, message):
        # Handle new messages
        device_id = self._device_channels[message.channel]
        _LOGGER.debug(
            "Received new messages on channel %s for device_id: %s with timetoken: %s: %s",
            message.channel,
            device_id,
            message.timetoken,
            message.message,
        )
        for callback in self._subscriptions:
            callback(
                device_id,
                datetime.datetime.fromtimestamp(
                    int(message.timetoken) / 10000000, tz=datetime.timezone.utc
                ),
                message.message,
            )

    def subscribe(self, update_callback):
        """Add an callback subscriber.

        Returns a callable that can be used to unsubscribe.
        """
        self._subscriptions.append(update_callback)

        def _unsubscribe():
            self._subscriptions.remove(update_callback)

        return _unsubscribe

    def register_device(self, device_detail):
        """Register a device to get updates."""
        if device_detail.pubsub_channel is None:
            return
        self._device_channels[device_detail.pubsub_channel] = device_detail.device_id

    @property
    def channels(self):
        """Return a list of registered channels."""
        return self._device_channels.keys()


def async_create_pubnub(user_uuid, subscriptions, brand=Brand.AUGUST):
    """Create a pubnub subscription."""
    tokens = PUBNUB_TOKENS[brand]
    pnconfig = PNConfiguration()
    pnconfig.subscribe_key = tokens["subscribe"]
    pnconfig.publish_key = tokens["publish"]
    pnconfig.uuid = f"pn-{str(user_uuid).upper()}"
    pnconfig.reconnect_policy = PNReconnectionPolicy.EXPONENTIAL
    pubnub = PubNub(pnconfig)
    pubnub.add_listener(subscriptions)
    pubnub.subscribe().channels(subscriptions.channels).execute()

    def _unsub():
        pubnub.unsubscribe().channels(subscriptions.channels).execute()

    return _unsub
