#!/usr/bin/env bash
# Start the hardware-free Nav2 loopback baseline: headless, no Gazebo, no RViz.
#
# The loopback sim publishes no map->odom transform until it gets /initialpose.
# Publish one within ~60 s of launch or Nav2 bringup aborts
# (see docs/hardware_free_demo.md).
set -euo pipefail

ROS_SETUP=/opt/ros/jazzy/setup.bash
if [[ ! -f "$ROS_SETUP" ]]; then
  echo "ERROR: $ROS_SETUP not found" >&2
  exit 1
fi

# setup.bash reads unset variables, so it can't be sourced under nounset.
set +u
source "$ROS_SETUP"
set -u

for pkg in nav2_bringup nav2_loopback_sim; do
  if ! ros2 pkg prefix "$pkg" >/dev/null 2>&1; then
    echo "ERROR: ROS package '$pkg' not found (apt: ros-jazzy-${pkg//_/-})" >&2
    exit 1
  fi
done

cmd=(ros2 launch nav2_bringup tb3_loopback_simulation.launch.py use_rviz:=False "$@")
echo "+ ${cmd[*]}"
exec "${cmd[@]}"
