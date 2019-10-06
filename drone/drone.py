import paho.mqtt.client as mqtt
import json
import urllib.parse
try:
    from command import *
except ImportError:
    from .command import *


class Drone:
    """
    这个类代表一架无人机。
    """
    def __init__(self, name):
        self.name = name
        self.min_height = 0
        self.max_height = 10

    def log(self, message):
        print("{} {}".format(self.name, message))

    def take_off(self):
        self.log("起飞")

    def land(self):
        self.log("降落")

    def land_in_safe_place(self):
        self.log("降落到指定地点")

    def move_up(self):
        self.log("爬高")

    def move_down(self):
        self.log("下降")

    def move_forward(self):
        self.log("向前飞")

    def move_back(self):
        self.log("向后飞")

    def move_left(self):
        self.log("向左飞")

    def move_right(self):
        self.log("向右飞")

    def rotate_left(self, degrees):
        self.log("向左旋转 {} 度".format(degrees))

    def rotate_right(self, degrees):
        self.log("向右旋转 {} 度".format(degrees))

    def set_min_height(self, meter):
        self.min_height = meter
        self.log("设置最小飞行高度 {} 米".format(meter))

    def set_max_height(self, meter):
        self.max_height = meter
        self.log("设置最大飞行高度 {} 米".format(meter))


class DroneCommandHandler:
    """
    这个类负责处理无人机接收到的消息并执行消息里携带的命令。当消息处理完毕之后，无人机把消息处理的结果发布到
    processedcommands/droneXXXX主题。
    """
    current_instance = None
    # 这架无人机订阅的消息主题
    commands_topic = ""
    # 处理完消息后，发送消息处理结果到下面的主题
    processed_commands_topic = ""

    def __init__(self, drone):
        self.drone = drone
        DroneCommandHandler.current_instance = self
        DroneCommandHandler.commands_topic = "commands/{}".format(drone.name)
        DroneCommandHandler.processed_commands_topic = "processedcommands/{}".format(drone.name)

        self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.client.on_connect = DroneCommandHandler.on_connect
        self.client.on_subscribe = DroneCommandHandler.on_subscribe
        self.client.on_message = DroneCommandHandler.on_message

        url = urllib.parse.urlparse(CLOUDMQTT_URL_STR)
        # 设置在 CloudMQTT 上的用户名和密码。我们的 MQTT 服务器托管在 CloudMQTT。
        self.client.username_pw_set(url.username, url.password)
        self.client.connect(host=url.hostname, port=url.port, keepalive=MQTT_KEEP_ALIVE)

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        print("连接到 MQTT 服务器")
        client.subscribe(topic=DroneCommandHandler.commands_topic, qos=2)
        # client.subscribe([("test_topic", 0),
        #                   (DroneCommandHandler.commands_topic, 2)])

    @staticmethod
    def on_subscribe(client, userdata, mid, granted_qos):
        print("已订阅主题。QoS = {}".format(granted_qos[0]))
        client.publish(topic=DroneCommandHandler.processed_commands_topic,
                       payload="无人机{}已订阅命令消息主题。".format(DroneCommandHandler.current_instance.name))

    @staticmethod
    def on_message(client, userdata, msg):
        if msg.topic == DroneCommandHandler.commands_topic:
            payload_string = msg.payload.decode('UTF-8')
            print("无人机{0}接收到命令消息: {1}".format(DroneCommandHandler.current_instance.drone.name,
                                              payload_string))
            try:
                message_dict = json.loads(payload_string)
                if COMMAND_KEY in message_dict:
                    command = message_dict[COMMAND_KEY]
                    drone = DroneCommandHandler.current_instance.drone

                    if command == CMD_TAKE_OFF:
                        drone.take_off()
                    elif command == CMD_LAND:
                        drone.land()
                    elif command == CMD_LAND_IN_SAFE_PLACE:
                        drone.land_in_safe_place()
                    elif command == CMD_MOVE_UP:
                        drone.move_up()
                    elif command == CMD_MOVE_DOWN:
                        drone.move_down()
                    elif command == CMD_MOVE_FORWARD:
                        drone.move_forward()
                    elif command == CMD_MOVE_BACK:
                        drone.move_back()
                    elif command == CMD_MOVE_LEFT:
                        drone.move_left()
                    elif command == CMD_MOVE_RIGHT:
                        drone.move_right()
                    elif command == CMD_ROTATE_RIGHT:
                        degrees = message_dict[KEY_DEGREES]
                        drone.rotate_right(degrees)
                    elif command == CMD_ROTATE_LEFT:
                        degrees = message_dict[KEY_DEGREES]
                        drone.rotate_left(degrees)
                    elif command == CMD_SET_MAX_HEIGHT:
                        meter = message_dict[KEY_METER]
                        drone.set_max_height(meter)
                    elif command == CMD_SET_MIN_HEIGHT:
                        meter = message_dict[KEY_METER]
                        drone.set_min_height(meter)

                    is_command_processed = True
                    if is_command_processed:
                        DroneCommandHandler.current_instance.publish_response_message(
                            message_dict)
                        print("{}确认命令执行成功....".format(DroneCommandHandler.current_instance.name))
                    else:
                        print("消息包含未定义命令:{}".format(command))
            except ValueError:
                print("消息包含不合法数据")

    def publish_response_message(self, message):
        """
        无人机执行消息里指定的命令后，发送命令成功执行的确认到processedcommands/droneXXXX主题
        """
        response_message = json.dumps({PROCESSED_COMMAND_KEY: message[COMMAND_KEY]})
        result = self.client.publish(
            topic=self.__class__.processed_commands_topic,
            payload=response_message)
        return result

    def process_commands(self):
        self.client.loop()


if __name__ == "__main__":
    drone = Drone("drone0001")
    drone_command_handler = DroneCommandHandler(drone)
    while True:
        drone_command_handler.process_commands()
