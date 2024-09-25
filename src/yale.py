#!/usr/bin/env python3

from datetime import datetime
import os
import re
import sys
import json
import asyncio
from aiohttp import ClientSession
import aiohttp
import yaml
from aiomqtt import Client as MqttClient, Will
import time
from signal import signal, SIGINT, SIGTERM
import logging
from yalexs.activity import ActivityType
from yalexs.api_async import ApiAsync as Api
from yalexs.authenticator_async import AuthenticatorAsync as Authenticator, AuthenticationState
from yalexs.authenticator_common import ValidationResult
from yalexs.exceptions import AugustApiAIOHTTPError as AugustApiHTTPError
from activity import ActivityStream
from pubnub_async import AugustPubNub, async_create_pubnub
from yalexs.pubnub_activity import activities_from_pubnub_message
from yalexs.const import Brand

CMD_LOCK = 'LOCK'
CMD_UNLOCK = 'UNLOCK'

STATE_JAMMED = 'JAMMED'
STATE_LOCKED = 'LOCKED'
STATE_LOCKING = 'LOCKING'
STATE_UNLOCKED = 'UNLOCKED'
STATE_UNLOCKING = 'UNLOCKING'

class GracefulKiller:
  def __init__(self):
    self.kill_now = asyncio.Event()
    # signal(SIGINT, self.exit_gracefully)
    # signal(SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now.set()

class MqttYale():

    data_dir = 'data'
    config_file = 'data/config.yml'
    topic_prefix = 'yale'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''

    mqtt_topic_map = {}
    locks = []
    _lock_by_id = {}

    _api_unsub_func1 = None
    _api_unsub_func2 = None

    http_session = None

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

         #Register program end event
        # atexit.register(self.programend)

        logging.info('init done')

    async def init_aiohttp_client(self):
        async with aiohttp.ClientSession() as session:
            pass

    def get_lock_db(self):
        try:
            with open(self.database_file, 'r') as f:
                lock_db = json.load(f)
        except Exception as e:
            if not isinstance(e, FileNotFoundError):
                logging.error('Could not read database file due to error: '+str(e))
            lock_db = {'locks': []}
        return lock_db
    
    def save_lock_db(self):
        lock_db = {
            'locks': [{
                'name': lock['name'],
                'mqtt_config_topic': lock['mqtt_config_topic']
            } for lock in self.locks]
        }
        with open(self.database_file, 'w') as f:
            json.dump(lock_db, f)

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'yale', 'data_dir', 'database_file']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                if key == 'database_file':
                    self.database_file = os.path.join(self.data_dir, 'database.json')

        self.yale.setdefault('access_token_cache_file', os.path.join(self.data_dir, 'access_cache.json'))
        self.yale.setdefault('login_method', 'email')

        self.availability_topic = self.topic_prefix + '/bridge/state'
        self.api_state_topic = self.topic_prefix + '/bridge/api'
        self.api_code_topic = self.api_state_topic + '/code'

    async def update_api_state(self):
        state = {'state': self.yale_authentication.state.value}

        if self.yale_authentication.state == AuthenticationState.REQUIRES_VALIDATION:
            state['validation_code_topic'] = self.api_code_topic

        await self.mqttclient.publish(self.api_state_topic, payload=json.dumps(state), qos=1, retain=True)

        if self.yale_authentication.state == AuthenticationState.AUTHENTICATED:
            await self.get_locks()

    async def yale_authenticate(self):
        if not self.http_session:
            self.http_session = ClientSession()
        try:
            if self.yale_authentication.state == AuthenticationState.AUTHENTICATED:
                return
            logging.info('Already authenticated, skipping authentication process')
        except AttributeError:
            pass

        logging.info('Authenticating with Yale API')
        if 'username' in self.yale and 'password' in self.yale:
            logging.debug('API credentials found')

        self.yale_api = Api(timeout=20, brand=Brand.YALE_HOME, aiohttp_session=self.http_session)
        self.yale_authenticator = Authenticator(self.yale_api, **self.yale)
        await self.yale_authenticator.async_setup_authentication()
        self.yale_authentication = await self.yale_authenticator.async_authenticate()

        logging.info('Authentication state: '+self.yale_authentication.state.value)

        if self.yale_authentication.state == AuthenticationState.REQUIRES_VALIDATION:
            await self.yale_authenticator.async_send_verification_code()
            logging.warning('Validation needed to log in, waiting for code at '+self.api_code_topic)
        elif self.yale_authentication.state == AuthenticationState.BAD_PASSWORD:
            logging.warning('Authentication failed: invalid credentials')

        await self.update_api_state()

    async def apply_validation_code(self, code):
        if self.yale_authentication == AuthenticationState.AUTHENTICATED:
            # skipping
            return
        
        validation_result = await self.yale_authenticator.async_validate_verification_code(code)
        if validation_result == ValidationResult.VALIDATED:
            logging.info('Code validation successful')
            self.yale_authentication = await self.yale_authenticator.async_authenticate()
        else:
            logging.error('Code validation unsuccessful, try new code or restart service to start over')

        await self.update_api_state()

    async def get_locks(self):
        self.api_user = await self.yale_api.async_get_user(self.yale_authentication.access_token)
        locks = await self.yale_api.async_get_locks(self.yale_authentication.access_token)
        logging.info(f'Found {len(locks)} locks')

        old_locks = self.locks
        self.locks = []
        self._lock_by_id = {}

        for lock in locks:
            id = re.sub('[^\w-]', '_', lock.device_name.lower().replace(' ', '_'))
            self.locks.append({
                'id': id,
                'device_id': lock.device_id,
                'name': lock.device_name,
                'house_id': lock.house_id,
                'mqtt_config_topic': '{}/lock/{}/config'.format(self.homeassistant_prefix, id),
                'mqtt_set_state_topic': '{}/{}/set'.format(self.topic_prefix, id),
                'mqtt_get_state_topic': '{}/{}/get'.format(self.topic_prefix, id),
                'mqtt_state_topic': '{}/{}'.format(self.topic_prefix, id),
                'mqtt_activity_topic': '{}/{}/activity'.format(self.topic_prefix, id),
                'mqtt_availability_topic': '{}/{}/availability'.format(self.topic_prefix, id),
                'details': await self.yale_api.async_get_lock_detail(self.yale_authentication.access_token, lock.device_id),
                'last_status': ''
            })

        self._lock_by_id = {l['device_id']: l for l in self.locks}

        self.mqtt_topic_map = {l['mqtt_set_state_topic']: l for l in self.locks}
        self.mqtt_topic_map.update({l['mqtt_get_state_topic']: l for l in self.locks})

        lock_ids = (l['id'] for l in self.locks)
        for lock in old_locks:
            if lock['id'] not in lock_ids:
                await self.mqtt_broadcast_lock_availability(lock, '')
                await self.mqttclient.unsubscribe(lock['mqtt_set_state_topic'])

        for lock in self.locks:
            #Configure MQTT for locks
            await self.configure_mqtt_for_lock(lock)

            #Broadcast current lock state to MQTT for locks
            await self.mqtt_broadcast_lock_availability(lock, '{"state": "online"}')
            await self.update_lock_state(lock)

            #Subscribe to MQTT lock updates
            await self.mqttclient.subscribe(lock['mqtt_set_state_topic'], 1)
            await self.mqttclient.subscribe('test', 1)

        mqtt_config_topics = [lock['mqtt_config_topic'] for lock in self.locks]
        for lock in self.get_lock_db()['locks']:
            if lock['mqtt_config_topic'] not in mqtt_config_topics:
                await self.remove_mqtt_config_for_lock(lock)
                await self.mqttclient.publish(lock['mqtt_config_topic'], '', 1, True)
        self.save_lock_db()

        self._activity_log = ActivityStream(self.yale_api, self.yale_authentication, self.locks[0]['house_id'], self.on_new_activity, last_update_time=datetime.now())

    async def api_subscribe(self):
        """Subscribe to pubnub messages."""
        await self.api_unsubscribe()
        logging.info('Subscribing to lock events')
        pubnub = AugustPubNub()
        for lock in self.locks:
            logging.debug('registering lock '+lock['details'].pubsub_channel)
            pubnub.register_device(lock['details'])

        loop = asyncio.get_event_loop()
        self._api_unsub_func1 = pubnub.subscribe(lambda *args: loop.call_soon_threadsafe(asyncio.create_task, self.on_api_message(*args)))
        self._api_unsub_func2 = async_create_pubnub(self.api_user['UserID'], pubnub, Brand.YALE_HOME)

    async def on_api_message(self, device_id, date_time, message):
        """Process a pubnub message."""
        logging.debug('Received lock events')
        lock = self._lock_by_id[device_id]
        activities = activities_from_pubnub_message(lock['details'], date_time, message)
        for act in activities:
            logging.debug(f'Lock event: {repr(act)}')
        self._activity_log._refresh()
        if self._activity_log.invalid_api_token:
            self.killer.exit_gracefully()
        await self.update_lock_state(lock)

    async def on_new_activity(self, activity):
        """Process a new activity."""
        logging.info(f'New activity: {repr(activity)}')

        lock = self._lock_by_id[activity.device_id]

        payload = {
            'action': activity.action,
            'time': activity.activity_start_time.isoformat()
        }

        if activity.activity_type == ActivityType.LOCK_OPERATION:
            payload['operated_by'] = activity.operated_by

        await self.mqttclient.publish(lock['mqtt_activity_topic'], json.dumps(payload), 1)   

    async def api_unsubscribe(self):
        """Stop the subscriptions."""

        if self._api_unsub_func1:
            logging.info('Unsubscribing from lock events (1)')
            self._api_unsub_func1()
            self._api_unsub_func1 = None

        if self._api_unsub_func2:
            logging.info('Unsubscribing from lock events (2)')
            self._api_unsub_func2()
            self._api_unsub_func2 = None

        logging.debug('Done unsubscribing')

    async def configure_mqtt_for_lock(self, lock):
        lock_configuration = {
            'name': lock['name'],
            "availability": [
                {'topic': self.availability_topic, 'value_template': '{{ value_jason.state }}'},
                {'topic': lock['mqtt_availability_topic'], 'value_template': '{{ value_jason.state }}'},
            ],
            "command_topic": lock['mqtt_set_state_topic'],
            "state_topic": lock['mqtt_state_topic'],
            "value_template": '{{ value_jason.state }}',
            "device": {
                "identifiers": [lock['device_id'], lock['details'].serial_number, lock['details'].mac_address],
                "manufacturer": "Yale",
                "model": "Yale Doorman",
                "hw_version": lock['details'].model,
                "name": lock['name'],
                "sw_version": lock['details'].firmware_version,
                "via_device": lock['details'].bridge.device_id
            },
            "unique_id": lock["device_id"]
        }

        json_conf = json.dumps(lock_configuration)
        logging.debug('Broadcasting homeassistant configuration for lock ' + lock['name'] + ': ' + json_conf)
        await self.mqttclient.publish(lock['mqtt_config_topic'], payload=json_conf, qos=1, retain=True)

    async def remove_mqtt_config_for_lock(self, lock):
        logging.debug('Removing homeassistant configuration for lock ' + lock['name'])
        await self.mqttclient.publish(lock['mqtt_config_topic'], payload='', qos=1, retain=True)

    def start(self):
        logging.info('starting')
        asyncio.run(self.main())

    async def mainloop(self):
        logging.debug('mainloop')
        while not self.killer.kill_now.is_set():
            # try:
            #     if self.yale_authentication.state == AuthenticationState.AUTHENTICATED and not self._api_unsub_func1:
            #         await self.api_subscribe()
            # except AttributeError:
            #     pass

            try:
                await asyncio.wait_for(self.killer.kill_now.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
            # self.killer.kill_now.wait(1)
        raise asyncio.CancelledError()

    async def main(self):
        logging.debug('main')

        #MQTT startup
        logging.info('Starting MQTT client')
        async with MqttClient(self.mqtt_server_ip, self.mqtt_server_port, username=self.mqtt_server_user, password=self.mqtt_server_password,
                                     will=Will(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)) as c:
            self.mqttclient = c
            logging.info('MQTT client started')
            
            await self.mqtt_on_connect()

            async with asyncio.TaskGroup() as tg:
                main_loop = tg.create_task(self.mainloop())
                mqtt_listener = tg.create_task(self.mqtt_listen())

                for sig in [SIGINT, SIGTERM]:
                    asyncio.get_event_loop().add_signal_handler(sig, tg._abort)

                logging.debug('Waiting for event loops to finish')
                try:
                    await asyncio.gather(*tg._tasks)
                except asyncio.CancelledError:
                    tg._abort()

                logging.debug('Shutting down now')

            await self.programend()

        time.sleep(0.1)
        logging.info('stopped')
        os._exit(0)

    async def programend(self):
        logging.info('stopping')

        await self.mqttclient.publish(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)
        for lock in self.locks:
            await self.mqtt_broadcast_lock_availability(lock, '')

        await self.api_unsubscribe()

    async def mqtt_on_connect(self):
        logging.info('MQTT client connected')

        await self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

        await self.mqttclient.subscribe(self.api_code_topic, 1)
        await self.mqttclient.subscribe('test', 1)

        for lock in self.locks:
            #Subscribe to MQTT lock updates
            await self.mqttclient.subscribe(lock['mqtt_set_state_topic'], 1)
            await self.mqttclient.subscribe(lock['mqtt_get_state_topic'], 1)

        await self.yale_authenticate()

    async def mqtt_listen(self):
        logging.debug('Listening for MQTT messages')
        async for msg in self.mqttclient.messages:
            logging.debug('Received MQTT message')
            payload_as_string = msg.payload.decode('utf-8')
            try:
                logging.debug('Received MQTT message on topic: ' + str(msg.topic) + ', payload: ' + msg.payload.decode('utf-8') + ', retained: ' + str(msg.retain))

                if str(msg.topic) == self.api_code_topic:
                    await self.apply_validation_code(int(payload_as_string))
                else:
                    try:
                        lock = self.mqtt_topic_map[str(msg.topic)]
                        if str(msg.topic).endswith('/get'):
                            await self.update_lock_state(lock)
                        else:
                            await self.set_lock_state(lock, payload_as_string)
                    except KeyError:
                        logging.error('Unknown command topic: '+str(msg.topic))

            except Exception as e:
                logging.exception(f'Encountered error when processing {str(msg.topic)} with payload "{payload_as_string}": '+str(e))

    async def set_lock_state(self, lock, cmd):
        cmd_upper = cmd.upper()
        if cmd_upper == CMD_LOCK:
            cmd_func = self.yale_api.async_lock

        elif cmd_upper == CMD_UNLOCK:
            cmd_func = self.yale_api.async_unlock

        elif cmd_upper == '':
            cmd_func = None

        else:
            logging.warning(f'Unknown command: {cmd}')
            return
        
        logging.info(f'Received "{cmd}" command from MQTT for {lock["name"]}')
        if cmd_func:
            try:
                await cmd_func(self.yale_authentication.access_token, lock['device_id'])
            except AugustApiHTTPError as e:
                logging.error(f'Failed to {cmd} {lock["name"]} due to error: {e}')

        await self.update_lock_state(lock)

    async def update_lock_state(self, lock):
        state = await self.yale_api.async_get_lock_status(self.yale_authentication.access_token, lock['device_id'], True)

        payload = json.dumps({
            'state': state[0].value.upper(),
            'doorState': state[1].value.upper()
        })

        if lock['last_status'] != payload:
            lock['last_status'] = payload
            await self.mqttclient.publish(lock['mqtt_state_topic'], payload, 1)

    async def mqtt_broadcast_lock_availability(self, lock, value):
        logging.debug('Broadcasting MQTT message on topic: ' + lock['mqtt_availability_topic'] + ', value: ' + value)
        await self.mqttclient.publish(lock['mqtt_availability_topic'], payload=value, qos=1, retain=True)

    async def mqtt_broadcast_state(self, lock):
        topic = lock['mqtt_state_topic']
        state = json.dumps(lock['control'].get_state())
        logging.debug('Broadcasting MQTT message on topic: ' + topic + ', value: ' + state)
        await self.mqttclient.publish(topic, payload=state, qos=1, retain=True)

if __name__ == '__main__':
    mqttYale =  MqttYale()
    mqttYale.start()
