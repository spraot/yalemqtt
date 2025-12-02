#!/usr/bin/env python3

import os
import re
import sys
import json
import asyncio
import yaml
from aiomqtt import Client as MqttClient, Will
import time
from signal import signal, SIGINT, SIGTERM
import logging
from seam import Seam
from fastapi import FastAPI, Request, Response
import uvicorn
from svix.webhooks import Webhook, WebhookVerificationError
from http import HTTPStatus
from urllib3.util.retry import Retry

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
    seam_api_key = ''
    webhook_secret = ''
    http_port = 8000
    http_host = '0.0.0.0'

    mqtt_topic_map = {}
    locks = []
    _lock_by_id = {}
    _lock_by_name = {}

    http_session = None
    app = None

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        self.seam = Seam(
            api_key=self.seam_api_key,
            wait_for_action_attempt={
                "timeout": 60.0,  # Increase timeout to 30 seconds
                "polling_interval": 1.0
            },
            retries=Retry(total=5, backoff_factor=2)
        )

        # Initialize FastAPI app
        self.app = FastAPI(title="Yale MQTT Bridge API", version="1.0.0")
        self._setup_routes()

         #Register program end event
        # atexit.register(self.programend)

        logging.info('init done')

    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        @self.app.post("/event")
        async def event(request: Request, response: Response):
            headers = request.headers
            payload = await request.body()

            logging.info(f'Event received: {payload}')
            try:
                wh = Webhook(self.webhook_secret)
                msg = wh.verify(payload, headers)
                logging.info(f'Event verified: {msg}')
            except WebhookVerificationError as e:
                logging.error(f'Event verification failed: {e}')
                response.status_code = HTTPStatus.BAD_REQUEST
                return

            for lock in self.locks:
                await self.update_lock_state(lock)
            return {"message": "Event received"}

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

        for key in ['topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'seam_api_key', 'webhook_secret', 'data_dir', 'database_file', 'http_port', 'http_host']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                if key == 'database_file':
                    self.database_file = os.path.join(self.data_dir, 'database.json')

        if not self.seam_api_key:
            raise ValueError('seam_api_key is required')

        self.availability_topic = self.topic_prefix + '/bridge/state'
        self.api_state_topic = self.topic_prefix + '/bridge/api'

    async def get_locks(self):
        locks = await asyncio.to_thread(self.seam.locks.list)
        logging.info(f'Found {len(locks)} locks')

        old_locks = self.locks
        self.locks = []
        self._lock_by_id = {}

        for lock in locks:
            logging.debug(f'Lock: {lock}')
            id = re.sub('[^\w-]', '_', lock.display_name.lower().replace(' ', '_'))
            self.locks.append({
                'id': id,
                'device_id': lock.device_id,
                'name': lock.nickname if lock.nickname else lock.display_name,
                'mqtt_config_topic': '{}/lock/{}/config'.format(self.homeassistant_prefix, id),
                'mqtt_set_state_topic': '{}/{}/set'.format(self.topic_prefix, id),
                'mqtt_get_state_topic': '{}/{}/get'.format(self.topic_prefix, id),
                'mqtt_state_topic': '{}/{}'.format(self.topic_prefix, id),
                'mqtt_error_topic': '{}/{}/error'.format(self.topic_prefix, id),
                'mqtt_activity_topic': '{}/{}/activity'.format(self.topic_prefix, id),
                'mqtt_availability_topic': '{}/{}/availability'.format(self.topic_prefix, id),
                'details': lock,
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

    async def on_api_message(self, device_id, date_time, message):
        """Process a pubnub message."""
        logging.debug('Received lock events')
        lock = self._lock_by_id[device_id]
        activities = activities_from_pubnub_message(lock['details'], date_time, message)
        for act in activities:
            logging.debug(f'Lock event: {repr(act)}')
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
                "identifiers": [lock['device_id']],
                "manufacturer": lock['details'].properties['manufacturer'],
                "model": "Yale Doorman",
                # "hw_version": lock['details'].model,
                "name": lock['name'],
                # "sw_version": lock['details'].firmware_version,
                # "via_device": lock['details'].bridge.device_id
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

        await self.get_locks()
        while not self.killer.kill_now.is_set():
            try:
                await asyncio.wait_for(self.killer.kill_now.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
        raise asyncio.CancelledError()

    async def main(self):
        logging.debug('main')

        # Start HTTP server
        logging.info(f'Starting HTTP server on {self.http_host}:{self.http_port}')
        http_config = uvicorn.Config(self.app, host=self.http_host, port=self.http_port, log_level=os.environ.get('LOGLEVEL', 'INFO').lower())
        http_server = uvicorn.Server(http_config)
        
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
                http_server_task = tg.create_task(http_server.serve())

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

    async def mqtt_on_connect(self):
        logging.info('MQTT client connected')

        await self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

        await self.mqttclient.subscribe('test', 1)

        for lock in self.locks:
            #Subscribe to MQTT lock updates
            await self.mqttclient.subscribe(lock['mqtt_set_state_topic'], 1)
            await self.mqttclient.subscribe(lock['mqtt_get_state_topic'], 1)

    async def mqtt_listen(self):
        logging.debug('Listening for MQTT messages')
        async for msg in self.mqttclient.messages:
            logging.debug('Received MQTT message')
            payload_as_string = msg.payload.decode('utf-8')
            try:
                logging.debug('Received MQTT message on topic: ' + str(msg.topic) + ', payload: ' + msg.payload.decode('utf-8') + ', retained: ' + str(msg.retain))

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
            cmd_func = self.seam.locks.lock_door

        elif cmd_upper == CMD_UNLOCK:
            cmd_func = self.seam.locks.unlock_door

        elif cmd_upper == '':
            cmd_func = None

        else:
            logging.warning(f'Unknown command: {cmd}')
            return
        
        logging.info(f'Received "{cmd}" command from MQTT for {lock["name"]}')
        if cmd_func:
            try:
                await asyncio.to_thread(cmd_func, device_id=lock['device_id'])
            except Exception as e:
                await self.handle_lock_error(lock, f'Failed to {cmd} {lock["name"]} due to error: {e}')

        await self.update_lock_state(lock)

    async def handle_lock_error(self, lock, error):
        logging.error(error)
        await self.mqttclient.publish(lock['mqtt_error_topic'], payload=json.dumps({'message': error}), qos=1)

    async def update_lock_state(self, lock):
        new_lock = await asyncio.to_thread(self.seam.locks.get, device_id=lock['device_id'])

        payload = json.dumps({
            'state': 'locked' if new_lock.properties["locked"] else 'unlocked',
            'doorState': 'open' if new_lock.properties["door_open"] else 'closed',
            'battery': new_lock.properties["battery_level"]
        })

        if lock['last_status'] != payload:
            lock['last_status'] = payload
            await self.mqttclient.publish(lock['mqtt_state_topic'], payload, 1)

        await self.mqtt_broadcast_lock_availability(lock, '{"state": "' + ('online' if lock['details'].properties['online'] else 'offline') + '"}')

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
