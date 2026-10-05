from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument("ROS_IP", default_value="0.0.0.0"),
            DeclareLaunchArgument("ROS_TCP_PORT", default_value="10000"),
            Node(
                package="ros_tcp_endpoint",
                executable="default_server_endpoint",
                emulate_tty=True,
                parameters=[
                    {"ROS_IP": LaunchConfiguration("ROS_IP")},
                    {"ROS_TCP_PORT": LaunchConfiguration("ROS_TCP_PORT")},
                ],
            )
        ]
    )
