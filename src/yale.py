#!/usr/bin/env python3

import os
import re
import sys
import json
import yaml
from statistics import mean
from threading import Event
import paho.mqtt.client as mqtt
import time
import signal
import logging
import atexit
from yalexs.api import Api 
from yalexs.authenticator import Authenticator, AuthenticationState
from yalexs.authenticator_common import ValidationResult
from yalexs.exceptions import AugustApiHTTPError
from pubnub_async import AugustPubNub, async_create_pubnub
from yalexs.pubnub_activity import activities_from_pubnub_message

CMD_LOCK = 'LOCK'
CMD_UNLOCK = 'UNLOCK'

STATE_JAMMED = 'JAMMED'
STATE_LOCKED = 'LOCKED'
STATE_LOCKING = 'LOCKING'
STATE_UNLOCKED = 'UNLOCKED'
STATE_UNLOCKING = 'UNLOCKING'

class GracefulKiller:
  def __init__(self):
    self.kill_now = Event()
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

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
    pump_topic = ''
    update_freq = 5

    mqtt_topic_map = {}
    locks = {}
    _lock_by_id = {}

    _api_unsub_func1 = None
    _api_unsub_func2 = None

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

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

    def update_api_state(self):
        self.mqttclient.publish(self.api_state_topic, payload=json.dumps({'state': self.yale_authentication.state.value}), qos=1, retain=True)

        if self.yale_authentication.state == AuthenticationState.AUTHENTICATED:
            self.get_locks()

    def yale_authenticate(self):
        try:
            if self.yale_authentication.state == AuthenticationState.AUTHENTICATED:
                return
            logging.info('Already authenticated, skipping authentication process')
        except AttributeError:
            pass

        logging.info('Authenticating with Yale API')
        if 'username' in self.yale and 'password' in self.yale:
            logging.debug('API credentials found')

        self.yale_api = Api(timeout=20)
        self.yale_authenticator = Authenticator(self.yale_api, **self.yale)
        self.yale_authentication = self.yale_authenticator.authenticate()

        logging.info('Authentication state: '+self.yale_authentication.state.value)

        if self.yale_authentication.state == AuthenticationState.REQUIRES_VALIDATION:
            self.yale_authenticator.send_verification_code()
            logging.warning('Validation needed to log in, waiting for code at '+self.api_code_topic)
        elif self.yale_authentication.state == AuthenticationState.BAD_PASSWORD:
            logging.warning('Authentication failed: invalid credentials')

        self.update_api_state()

    def apply_validation_code(self, code):
        if self.yale_authentication == AuthenticationState.AUTHENTICATED:
            # skipping
            return
        
        validation_result = self.yale_authenticator.validate_verification_code(code)
        if validation_result == ValidationResult.VALIDATED:
            logging.info('Code validation successful')
            self.yale_authentication = self.yale_authenticator.authenticate()
        else:
            logging.error('Code validation unsuccessful, try new code or restart service to start over')

        self.update_api_state()

    def get_locks(self):
        self.api_user = self.yale_api.get_user(self.yale_authentication.access_token)
        locks = self.yale_api.get_locks(self.yale_authentication.access_token)
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
                'mqtt_state_topic': '{}/{}'.format(self.topic_prefix, id),
                'mqtt_availability_topic': '{}/{}/availability'.format(self.topic_prefix, id),
                'details': self.yale_api.get_lock_detail(self.yale_authentication.access_token, lock.device_id)
            })

        self._lock_by_id = {l['device_id']: l for l in self.locks}

        self.mqtt_topic_map = {l['mqtt_set_state_topic']: l for l in self.locks}

        lock_ids = (l['id'] for l in self.locks)
        for lock in old_locks:
            if lock['id'] not in lock_ids:
                self.mqtt_broadcast_lock_availability(lock, '')
                self.mqttclient.unsubscribe(lock['mqtt_set_state_topic'])

        for lock in self.locks:
            #Configure MQTT for locks
            self.configure_mqtt_for_lock(lock)

            #Broadcast current lock state to MQTT for locks
            self.mqtt_broadcast_lock_availability(lock, '{"state": "online"}')
            self.update_lock_state(lock)

            #Subsribe to MQTT lock updates
            self.mqttclient.subscribe(lock['mqtt_set_state_topic'], 1)

        mqtt_config_topics = [lock['mqtt_config_topic'] for lock in self.locks]
        for lock in self.get_lock_db()['locks']:
            if lock['mqtt_config_topic'] not in mqtt_config_topics:
                self.remove_mqtt_config_for_lock(lock)
                self.mqttclient.publish(lock['mqtt_config_topic'], '', 1, True)
        self.save_lock_db()

    def api_subscribe(self):
        """Subscribe to pubnub messages."""
        self.api_unsubscribe()
        logging.info('Subscribing to lock events')
        pubnub = AugustPubNub()
        for lock in self.locks:
            logging.debug('registering lock '+lock['details'].pubsub_channel)
            pubnub.register_device(lock['details'])
        self._api_unsub_func1 = pubnub.subscribe(self.on_api_message)
        self._api_unsub_func2 = async_create_pubnub(self.api_user['UserID'], pubnub)

    def on_api_message(self, device_id, date_time, message):
        """Process a pubnub message."""
        logging.debug('Received lock events')
        lock = self._lock_by_id[device_id]
        activities = activities_from_pubnub_message(lock['details'], date_time, message)
        for act in activities:
            logging.info(f'Lock event: {repr(act)}')
        
        self.update_lock_state(lock)

    def api_unsubscribe(self):
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

    def configure_mqtt_for_lock(self, lock):
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
        self.mqttclient.publish(lock['mqtt_config_topic'], payload=json_conf, qos=1, retain=True)

    def remove_mqtt_config_for_lock(self, lock):
        logging.debug('Removing homeassistant configuration for lock ' + lock['name'])
        self.mqttclient.publish(lock['mqtt_config_topic'], payload='', qos=1, retain=True)

    def start(self):
        logging.info('starting')

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logging.info('MQTT client started')

        logging.info('Starting main thread')
        # self.main_thread = threading.Thread(name='main', target=self.main)
        # self.main_thread.start()

        self.main()

        # logging.info('started')

    def main(self):
        logging.debug('main')
        while not self.killer.kill_now.is_set():
            try:
                if self.yale_authentication.state == AuthenticationState.AUTHENTICATED and not self._api_unsub_func1:
                    # self.subscribe_thread = threading.Thread(name='api', target=self.api_subscribe)
                    # self.subscribe_thread.start()
                    # asyncio.set_event_loop(asyncio.new_event_loop())
                    self.api_subscribe()
            except AttributeError:
                pass
            self.killer.kill_now.wait(1)

        # self.api_unsubscribe()
        logging.debug('Shutting down now')
        self.programend()
        os._exit(0)

    def programend(self):
        logging.info('stopping')

        self.mqttclient.publish(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)
        for lock in self.locks:
            self.mqtt_broadcast_lock_availability(lock, '')

        self.api_unsubscribe()

        time.sleep(0.5)
        self.mqttclient.disconnect()
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info('MQTT client connected with result code '+str(rc))

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)

        self.mqttclient.subscribe(self.api_code_topic, 1)

        self.yale_authenticate()

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            if str(msg.topic) == self.api_code_topic:
                self.apply_validation_code(int(payload_as_string))
            else:
                self.set_lock_state(self.mqtt_topic_map[str(msg.topic)], payload_as_string)

        except Exception as e:
            logging.exception(f'Encountered error when processing {msg.topic} with payload "{payload_as_string}": '+str(e))

    def set_lock_state(self, lock, cmd):
        cmd_upper = cmd.upper()
        if cmd_upper == CMD_LOCK:
            cmd_func = self.yale_api.lock

        elif cmd_upper == CMD_UNLOCK:
            cmd_func = self.yale_api.unlock

        elif cmd_upper == '':
            cmd_func = None

        else:
            logging.warning(f'Unknown command: {cmd}')
            return
        
        logging.info(f'Received "{cmd}" command from MQTT for {lock["name"]}')
        if cmd_func:
            try:
                cmd_func(self.yale_authentication.access_token, lock['device_id'])
            except AugustApiHTTPError as e:
                logging.error(f'Failed to {cmd} {lock["name"]} due to error: {e}')

        self.update_lock_state(lock)

    def update_lock_state(self, lock):
        state = self.yale_api.get_lock_status(self.yale_authentication.access_token, lock['device_id'], True)

        payload = json.dumps({
            'state': state[0].value.upper(),
            'doorState': state[1].value.upper()
        })

        self.mqttclient.publish(lock['mqtt_state_topic'], payload, 1)        

    def mqtt_broadcast_lock_availability(self, lock, value):
       logging.debug('Broadcasting MQTT message on topic: ' + lock['mqtt_availability_topic'] + ', value: ' + value)
       self.mqttclient.publish(lock['mqtt_availability_topic'], payload=value, qos=1, retain=True)

    def mqtt_broadcast_state(self, lock):
        topic = lock['mqtt_state_topic']
        state = json.dumps(lock['control'].get_state())
        logging.debug('Broadcasting MQTT message on topic: ' + topic + ', value: ' + state)
        self.mqttclient.publish(topic, payload=state, qos=1, retain=True)

if __name__ == '__main__':
    mqttYale =  MqttYale()
    mqttYale.start()
