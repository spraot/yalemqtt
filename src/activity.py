"""Consume the august activity stream."""
import logging

from aiohttp import ClientError

from debounce import debounce

_LOGGER = logging.getLogger(__name__)

ACTIVITY_STREAM_FETCH_LIMIT = 20
ACTIVITY_CATCH_UP_FETCH_LIMIT = 2500


class ActivityStream():
    """August activity stream handler."""

    def __init__(self, api, auth, house_id, on_device_update, last_update_time=None):
        """Init August activity stream object."""
        self._api = api
        self._house_id = house_id
        self._latest_activities = {}
        self._last_update_time = last_update_time
        self._on_device_update = on_device_update
        self._auth = auth

    def _refresh(self, time):
        """Update the activity stream from August."""
        # This is the only place we refresh the api token
        self._api.refresh_access_token_if_needed()
        self._update_activities(time)

    @debounce(1)
    def _update_activities(self):
        """Update device activities for a house."""
        _LOGGER.debug("Start retrieving device activities")

        limit = ACTIVITY_STREAM_FETCH_LIMIT

        _LOGGER.debug("Updating device activity for house id %s", self._house_id)
        try:
            activities = self._api.get_house_activities(
                self._auth.access_token, self._house_id, limit=limit
            )
        except ClientError as ex:
            _LOGGER.error(
                "Request error trying to retrieve activity for house id %s: %s",
                self._house_id,
                ex,
            )
            # Make sure we process the next house if one of them fails
            return

        _LOGGER.debug(
            "Completed retrieving device activities for house id %s", self._house_id
        )

        updated_device_ids = self.process_newer_device_activities(activities)

        if not updated_device_ids:
            return

        for device_id in updated_device_ids:
            _LOGGER.debug(
                "signal_device_id_update (from activity stream): %s",
                device_id,
            )

    def process_newer_device_activities(self, activities):
        """Process activities if they are newer than the last one."""
        updated_device_ids = set()
        for activity in activities[::-1]:
            device_id = activity.device_id
            lastest_activity = self._latest_activities.get(device_id)
            if lastest_activity:
                lastest_activity_activity_start_time = lastest_activity.activity_start_time
            else:
                lastest_activity_activity_start_time = self._last_update_time

            # Ignore activities that are older than the latest one
            if (
                lastest_activity_activity_start_time
                and lastest_activity_activity_start_time >= activity.activity_start_time
            ):
                continue

            self._latest_activities[device_id] = activity

            if self._on_device_update:
                try:
                    self._on_device_update(activity)
                except Exception as e:  # pylint: disable=broad-except
                    _LOGGER.exception('Error calling on_device_update for new activity: %s', e)

            updated_device_ids.add(device_id)

        return updated_device_ids