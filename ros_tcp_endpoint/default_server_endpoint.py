#!/usr/bin/env python

import rclpy

from ros_tcp_endpoint import TcpServer


def main(args=None):
    rclpy.init(args=args)
    tcp_server = TcpServer("UnityEndpoint")

    tcp_server.start()

    tcp_server.setup_executor()

    tcp_server.destroy_nodes()
    # Ctrl-C shuts the context down from rclpy's own signal handler, and
    # shutting down an already shut down context raises rather than doing
    # nothing -- which turned every clean stop into a traceback and a non-zero
    # exit code.
    if rclpy.ok():
        rclpy.shutdown()


if __name__ == "__main__":
    main()
