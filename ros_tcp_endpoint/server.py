#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import rclpy
import socket
import json
import sys
import threading
import importlib

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor
from rclpy.serialization import deserialize_message
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService

# Smallest MultiThreadedExecutor pool to run with. See setup_executor.
MIN_EXECUTOR_THREADS = 8


class TcpServer(Node):
    """
    Initializes ROS node and TCP server.
    """

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        super().__init__(node_name)

        self.declare_parameter("ROS_IP", "0.0.0.0")
        self.declare_parameter("ROS_TCP_PORT", 10000)

        if tcp_ip:
            self.loginfo("Using ROS_IP override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = self.get_parameter("ROS_IP").get_parameter_value().string_value

        if tcp_port:
            self.loginfo("Using ROS_TCP_PORT override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = self.get_parameter("ROS_TCP_PORT").get_parameter_value().integer_value

        self.unity_tcp_sender = UnityTcpSender(self)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        # Registrations arrive on a client thread while the executor spins on
        # the main one, and two Unity clients connecting at once would other-
        # wise race over the same topic's entry -- one destroying the node the
        # other had just put in the table.
        self.registration_lock = threading.RLock()

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((self.tcp_ip, self.tcp_port))

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                ClientThread(conn, self, ip, port).start()
            except socket.timeout as err:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

    def send_unity_error(self, error):
        self.unity_tcp_sender.send_unity_error(error)

    def send_unity_message(self, topic, message):
        self.unity_tcp_sender.send_unity_message(topic, message)

    def send_unity_service(self, topic, service_class, request):
        return self.unity_tcp_sender.send_unity_service_request(topic, service_class, request)

    def send_unity_service_response(self, srv_id, data):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data)

    def handle_syscommand(self, topic, data):
        function = getattr(self.syscommands, topic[2:])
        if function is None:
            self.send_unity_error("Don't understand SysCommand.'{}'".format(topic))
        else:
            message_json = data.decode("utf-8")[:-1]
            params = json.loads(message_json)
            function(**params)

    def loginfo(self, text):
        self.get_logger().info(text)

    def logwarn(self, text):
        self.get_logger().warning(text)

    def logerr(self, text):
        self.get_logger().error(text)

    def setup_executor(self):
        """
            Since rclpy.spin() is a blocking call the server needed a way
            to spin all of the relevant nodes at the same time.

            MultiThreadedExecutor allows us to set the number of threads
            needed as well as the nodes that need to be spun.
        """
        # The tables are empty until Unity connects and registers, so sizing
        # the pool from them alone leaves one worker for the whole run. A Unity
        # service call blocks its worker until Unity answers (see
        # UnityTcpSender.send_unity_service_request), and with a single worker
        # that stalls every other callback with it, so keep a floor.
        num_threads = max(
            MIN_EXECUTOR_THREADS,
            len(self.publishers_table.keys())
            + len(self.subscribers_table.keys())
            + len(self.ros_services_table.keys())
            + len(self.unity_services_table.keys())
            + 1,
        )
        executor = MultiThreadedExecutor(num_threads)

        executor.add_node(self)

        for ros_node in self.publishers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.subscribers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.ros_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_services_table.values():
            executor.add_node(ros_node)

        self.executor = executor
        self.spin_until_shutdown(executor)

    def spin_until_shutdown(self, executor):
        """
            executor.spin(), but surviving a node torn down mid-wait.

            rclpy builds its wait set from a snapshot of the executor's nodes:
            it checks each waitable is alive, and adds it to the set a few
            lines later. A node destroyed in that window -- which is what
            re-registering a topic does, from the client thread -- makes the
            add raise InvalidHandle straight out of spin(), and the endpoint
            exits. Registering is ordered to keep that window shut (see
            unregister_node) and a reconnect no longer churns nodes at all, so
            landing here should be rare; it is caught anyway, because the cost
            is a whole Unity session and the recovery is only to build the wait
            set again. wait_for_ready_callbacks discards the generator that
            raised and makes a new one on the next call, so continuing is
            enough -- there is no state left to repair.
        """
        while rclpy.ok():
            try:
                executor.spin_once()
            except ExternalShutdownException:
                return
            except _rclpy.InvalidHandle:
                self.logwarn(
                    "a node was destroyed while the wait set was being built; continuing"
                )

    def unregister_node(self, old_node):
        """
            Take a node out of the executor, then tear it down.

            Not the other way round: destroying first leaves the executor
            holding entities whose handles are already gone, which is what used
            to take the endpoint down on a Unity reconnect.
        """
        if old_node is None:
            return
        if self.executor is not None:
            self.executor.remove_node(old_node)
        old_node.unregister()

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
        for ros_node in self.publishers_table.values():
            ros_node.destroy_node()
        for ros_node in self.subscribers_table.values():
            ros_node.destroy_node()
        for ros_node in self.ros_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.unity_services_table.values():
            ros_node.destroy_node()

        self.destroy_node()


class SysCommands:
    def __init__(self, tcp_server):
        self.tcp_server = tcp_server

    def register(self, table, topic, signature, create):
        """
            Put the node for `topic` into `table`, reusing what is there.

            Unity re-sends its whole registration list every time it connects,
            so on a reconnect each of these calls asks for a node identical to
            the one already running. Building the replacement means destroying
            a live node -- twenty-odd of them in a row -- while the executor is
            spinning on it, and racing the wait set it is part of. When the
            registration is the same as the one in place, leave the node alone:
            nothing about it needs to change, and the executor's entities stay
            put across the reconnect.

            `signature` is everything that would make a different node, so an
            unchanged one is only ever reused for an unchanged request.
        """
        with self.tcp_server.registration_lock:
            old_node = table.get(topic)
            if getattr(old_node, "registration", None) == signature:
                return
            # Destroyed before the replacement is created, not after: both
            # carry the same node name, derived from the topic.
            self.tcp_server.unregister_node(old_node)
            new_node = create()
            new_node.registration = signature
            table[topic] = new_node
            if self.tcp_server.executor is not None:
                self.tcp_server.executor.add_node(new_node)

    def subscribe(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        self.register(
            self.tcp_server.subscribers_table,
            topic,
            ("subscribe", message_class),
            lambda: RosSubscriber(topic, message_class, self.tcp_server),
        )

        self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def publish(self, topic, message_name, queue_size=10, latch=False):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        self.register(
            self.tcp_server.publishers_table,
            topic,
            ("publish", message_class, queue_size, latch),
            lambda: RosPublisher(topic, message_class, queue_size=queue_size, latch=latch),
        )

        self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        self.register(
            self.tcp_server.ros_services_table,
            topic,
            ("ros_service", message_class),
            lambda: RosService(topic, message_class),
        )

        self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        self.register(
            self.tcp_server.unity_services_table,
            topic,
            ("unity_service", message_class),
            lambda: UnityService(str(topic), message_class, self.tcp_server),
        )

        self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def response(self, srv_id):  # the next message is a service response
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = False

    def request(self, srv_id):  # the next message is a service request
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = True

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list()

    def resolve_message_name(self, name, extension="msg"):
        try:
            names = name.split("/")
            module_name = names[0]
            class_name = names[1]
            importlib.import_module(module_name + "." + extension)
            module = sys.modules[module_name]
            if module is None:
                self.tcp_server.logerr("Failed to resolve module {}".format(module_name))
            module = getattr(module, extension)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}".format(module_name, extension)
                )
            module = getattr(module, class_name)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}.{}".format(module_name, extension, class_name)
                )
            return module
        except (IndexError, KeyError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None
