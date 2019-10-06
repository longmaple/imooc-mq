import paho.mqtt.client as mqtt
import json
import urllib.parse
try:
    from command import *
except ImportError:
    from .command import *

drone_name = "drone0001"
commands_topic = "commands/{}".format(drone_name)
processed_commands_topic = "processedcommands/{}".format(drone_name)


class LoopControl:
    is_last_command_processed = False


def on_connect(client, userdata, flags, rc):
    print("与 MQTT 服务器连接: {}".format(mqtt.connack_string(rc)))
    client.subscribe(processed_commands_topic)


def on_subscribe(client, userdata, mid, granted_qos):
    print("已订阅主题。QoS = {}".format(granted_qos[0]))


def on_message(client, userdata, msg):
    if msg.topic == processed_commands_topic:
        payload_string = msg.payload.decode('UTF-8')
        print(payload_string)
        if payload_string.count(CMD_LAND_IN_SAFE_PLACE) > 0:
            LoopControl.is_last_command_processed = True


def publish_command(client, command_name, key="", value=""):
    if key:
        command_message = json.dumps({
            COMMAND_KEY: command_name,
            key: value})
    else:
        command_message = json.dumps({COMMAND_KEY: command_name})
    result = client.publish(topic=commands_topic,
                            payload=command_message, qos=2)
    return result


if __name__ == "__main__":
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    url = urllib.parse.urlparse(CLOUDMQTT_URL_STR)
    client.username_pw_set(url.username, url.password)
    client.connect(host=url.hostname, port=url.port, keepalive=MQTT_KEEP_ALIVE)

    publish_command(client, CMD_TAKE_OFF)
    publish_command(client, CMD_MOVE_UP)
    publish_command(client, CMD_ROTATE_RIGHT, KEY_DEGREES, 90)
    publish_command(client, CMD_ROTATE_LEFT, KEY_DEGREES, 45)
    publish_command(client, CMD_ROTATE_LEFT, KEY_DEGREES, 45)
    publish_command(client, CMD_LAND_IN_SAFE_PLACE)

    while not LoopControl.is_last_command_processed:
        client.loop()
    client.disconnect()
    print("end of loop")