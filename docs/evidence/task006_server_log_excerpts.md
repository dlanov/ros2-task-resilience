# Task 006: server log excerpts

Small, sanitized excerpts of Nav2 server-side logs from the 2026-09-23 experiment runs of the ROS
action recovery spike (`docs/ros_action_recovery_spike.md`). They exist because the client-side
outputs of those runs were not found. Only a few lines are quoted. The original logs were read
and not changed, and they are not committed.

## What these excerpts show and do not show

They show only what the quoted lines say:

- Eight `component_container_isolated` death records with exit code -6 exist in the launch logs of
  2026-09-23 (E1).
- The container log of the retention run has a `Goal succeeded` entry at 21:08:41.139
  local time, which is the time the original report used as the anchor for its retention offsets.
  Two later `Begin navigating` entries follow it (E2).
- For one abort run, the launch-log death record is at 21:14:13.367 (E3).

They do not show:

- **A per-trial mapping of the aborts.** Eight retained abort records are not a recovered
  per-trial mapping. Their number equals the reported 7 planned duplicate trials plus 1 unplanned
  instance, and that is all that ties them together. No record carries a trial label, a goal UUID
  or what was submitted, so none can be assigned to experiment 5a, 5b, 6 or the unplanned
  instance.
- **Dispatch-level correlation.** None of the goal log lines (`Begin navigating`,
  `Goal succeeded`) contains a goal UUID. No entry can be tied to a particular UUID or dispatch.
  Reading the E2 entries as the retention goal or as later goals rests on the original report and
  on timing only.
- **Client-side facts.** Without the client output, these logs cannot check acceptance
  responses, `UNKNOWN` replies, status-topic listings or the exact retention checkpoints (result
  retrievable at 882.5 s, `UNKNOWN` at 902.2 s). Those stay as originally reported.
- **The exception text.** `terminate called after throwing an instance of 'std::runtime_error'`
  and `what():  Failed to accept new goal` exist in the original report. They do not occur in
  any retained file from the evening runs (see Method). The only file of that day that contains
  the phrase `Failed to accept new goal` is an earlier one with a different message (E4).
- **Outcomes per goal.** Nothing here is a per-goal result table. Navigation, success or
  cancellation messages were not counted or matched into one terminal outcome per goal, which
  would need a correlation these logs do not support.

## Method

- The files were found under `~/.ros/log`, the ROS 2 default log directory (`ROS_LOG_DIR` and
  `ROS_HOME` were unset in the shell used). Each is identified by its run-directory name (start
  time, host, process id of `ros2 launch`) and by its SHA-256 below.
- Read-only. A script picked the lines below by content and wrote them here. A separate check
  then re-read the source files, parsed this document, and compared all 19
  quoted lines and every size and SHA-256 above with the sources: no differences.
- Line numbers are 1-based line numbers in the source file. In the blocks, the prefix
  `ID:Lnn|` was added by this document (source ID and source line number). The text after `|` is
  the source line.
- Raw times are seconds since the Unix epoch. "Local time" annotations use the machine's time
  zone at the time (CDT, UTC-05:00), truncated to milliseconds. They are annotations only. The
  raw lines are not modified.
- Exception-text search: a fixed-string search for `terminate called` and
  `Failed to accept new goal` over every file under `~/.ros/log` modified on 2026-09-23
  (401 files) matched in one file only, `X1` (E4), written at 10:46:54.189. There was no
  match in the 300 files modified from 20:00 on, which include every launch log and
  container log of the evening runs.

## Sources

| ID | Source file | Bytes | SHA-256 |
|---|---|---|---|
| A1 | `~/.ros/log/2026-09-23-21-01-46-866896-<HOST>-136653/launch.log` | 3044 | `65f6e4dd4add13448397eacd7d31a4e1a8933c8dcdaf496ba2f655098d11713e` |
| A2 | `~/.ros/log/2026-09-23-21-08-35-574719-<HOST>-137654/launch.log` | 3044 | `5784a8bb7b43adcfba21a6c66ddbfd1517e98fcb5ac61b68a127aa67cd9217e4` |
| A3 | `~/.ros/log/2026-09-23-21-10-48-047517-<HOST>-138633/launch.log` | 3044 | `55fd01271aed9ef201d23e91acb9f43313b0b1d920a57cf1ea695423533404ef` |
| A4 | `~/.ros/log/2026-09-23-21-13-24-160422-<HOST>-139084/launch.log` | 3044 | `76b5817fb309cdc0e9c7b7586a81a585776c6929c11f5d0bd88645dd573525e7` |
| A5 | `~/.ros/log/2026-09-23-21-22-56-339509-<HOST>-141506/launch.log` | 3044 | `44427b3900728afbbb694b88ec32a3941a70d9b9b7d6181b812e9f045615eb8a` |
| A6 | `~/.ros/log/2026-09-23-21-38-00-276492-<HOST>-144487/launch.log` | 3044 | `5008cb01afa602ef5802a55c274ea7642a24abac376f1aa50bd315efe15fcfb7` |
| A7 | `~/.ros/log/2026-09-23-21-38-28-587619-<HOST>-144831/launch.log` | 3044 | `67dba64060adff40da2a31a071feb33bb50d2ce3182148458840f0c000afb0a0` |
| A8 | `~/.ros/log/2026-09-23-21-39-08-453017-<HOST>-145183/launch.log` | 3044 | `a32bed5ca6b93ae53865e5b39c65d998ed41152b3fef17cd32d8d0bf60e6ff91` |
| L1 | `~/.ros/log/2026-09-23-21-07-48-370566-<HOST>-137420/launch.log` | 3045 | `783666c8beeb8621614d259fce7de46b45941b341f3d3fab3d922aef8bb75a50` |
| R1 | `~/.ros/log/component_container_isolated_137445_1790215669190.log` | 52549 | `6d6ebd1231f40a3262ac974b16e95ae3c085072b3cad1696e6ded60be7b9304f` |
| C1 | `~/.ros/log/component_container_isolated_139109_1790216005067.log` | 28422 | `cc6a30f93122b202dde8c5b5170e76001ff8055c93424a99d8364e4c3ba584d1` |
| X1 | `~/.ros/log/python3_6626_1790178352444.log` | 398 | `270007d386720bb562dd0a085602d78a33964f0886cf0445c44f3c3f554e1f12` |

`A1` to `A8` are the launch logs of the eight runs with a container death record of exit code -6,
in time order. `L1` is the launch log and `R1` the container (`nav2_container`) log of the
retention run. `C1` is the container log of the run in E3. `X1` is the one other file that contains
the phrase `Failed to accept new goal` (E4).

## E1: container death records with exit code -6

```text
A1:L19| 1790215559.4983368 [ERROR] [component_container_isolated-5]: process has died [pid 136678, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_vyou2739 --params-file /tmp/launch_params_k1c7plim -r /tf:=tf -r /tf_static:=tf_static'].
A2:L19| 1790215748.3316126 [ERROR] [component_container_isolated-5]: process has died [pid 137681, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_s6_q8umb --params-file /tmp/launch_params_vagztnx8 -r /tf:=tf -r /tf_static:=tf_static'].
A3:L19| 1790215875.7849839 [ERROR] [component_container_isolated-5]: process has died [pid 138658, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_o44ef3u5 --params-file /tmp/launch_params___oi8siv -r /tf:=tf -r /tf_static:=tf_static'].
A4:L19| 1790216053.3670843 [ERROR] [component_container_isolated-5]: process has died [pid 139109, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_kh5nnry4 --params-file /tmp/launch_params_l23ya3uk -r /tf:=tf -r /tf_static:=tf_static'].
A5:L19| 1790216647.4554126 [ERROR] [component_container_isolated-5]: process has died [pid 141531, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_bbdz546d --params-file /tmp/launch_params_tp_lwjlv -r /tf:=tf -r /tf_static:=tf_static'].
A6:L22| 1790217504.4064841 [ERROR] [component_container_isolated-5]: process has died [pid 144512, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_hw9zosev --params-file /tmp/launch_params_muba_ysn -r /tf:=tf -r /tf_static:=tf_static'].
A7:L22| 1790217544.5467610 [ERROR] [component_container_isolated-5]: process has died [pid 144856, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_bjtmakrf --params-file /tmp/launch_params_2uqydfs7 -r /tf:=tf -r /tf_static:=tf_static'].
A8:L19| 1790217573.4430618 [ERROR] [component_container_isolated-5]: process has died [pid 145208, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_70w62wgm --params-file /tmp/launch_params_ef52xozv -r /tf:=tf -r /tf_static:=tf_static'].
```

| ID | Run start (local) | Death record (local) | Raw time | Line |
|---|---|---|---|---|
| A1 | 21:01:46 | 21:05:59.498 | 1790215559.4983368 | 19 |
| A2 | 21:08:35 | 21:09:08.331 | 1790215748.3316126 | 19 |
| A3 | 21:10:48 | 21:11:15.784 | 1790215875.7849839 | 19 |
| A4 | 21:13:24 | 21:14:13.367 | 1790216053.3670843 | 19 |
| A5 | 21:22:56 | 21:24:07.455 | 1790216647.4554126 | 19 |
| A6 | 21:38:00 | 21:38:24.406 | 1790217504.4064841 | 22 |
| A7 | 21:38:28 | 21:39:04.546 | 1790217544.5467610 | 22 |
| A8 | 21:39:08 | 21:39:33.443 | 1790217573.4430618 | 19 |

Limited claim: these eight records exist. The 25 launch directories of 2026-09-23
(two have an empty launch log) hold 22 container death records in total. Exactly these
eight have exit code -6. The other 14 have exit code -11 (13) and exit code -9 (1). Exit code -11 at shutdown
after SIGINT is described in `docs/hardware_free_demo.md` (problem 5).

## E2: the retention run

```text
R1:L259| [INFO] [1790215706.986385429] [bt_navigator]: Begin navigating from current location (-2.00, -0.50) to (0.50, 0.50)
R1:L260| [INFO] [1790215707.008225830] [controller_server]: Received a goal, begin computing control effort.
R1:L276| [INFO] [1790215721.111168441] [controller_server]: Reached the goal!
R1:L278| [INFO] [1790215721.139593768] [bt_navigator]: Goal succeeded
R1:L305| [INFO] [1790217177.298416789] [bt_navigator]: Begin navigating from current location (0.28, 0.55) to (-2.00, -0.50)
R1:L326| [INFO] [1790217205.752963499] [bt_navigator]: Begin navigating from current location (-1.86, -0.41) to (0.50, 0.50)
L1:L23| 1790217599.9892185 [ERROR] [component_container_isolated-5]: process has died [pid 137445, exit code -11, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_yq_slr3q --params-file /tmp/launch_params_0f5650mh -r /tf:=tf -r /tf_static:=tf_static'].
```

| Line | Local time | Entry |
|---|---|---|
| R1:L259 | 21:08:26.986 | `Begin navigating` (first goal in this log) |
| R1:L260 | 21:08:27.008 | controller `Received a goal` |
| R1:L276 | 21:08:41.111 | controller `Reached the goal!` |
| R1:L278 | 21:08:41.139 | `Goal succeeded` (offset 0: the anchor) |
| R1:L305 | 21:32:57.298 | `Begin navigating`, +1456.16 s after the anchor |
| R1:L326 | 21:33:25.752 | `Begin navigating`, +1484.61 s after the anchor |
| L1:L23 | 21:39:59.989 | container death record, exit -11, +1878.85 s after the anchor |

Limited claims:

- The `Goal succeeded` entry is at 21:08:41.139, matching the anchor
  (21:08:41.139) that the original report used for its retention offsets. This corroborates the
  anchor. It does not corroborate the client-side checkpoints measured from it.
- Two later `Begin navigating` entries exist at +1456.16 s and +1484.61 s after the anchor. The
  original report describes an unrelated goal accepted at about +1456 s and a resubmission of the
  original UUID after retention expired. The timing is consistent with that description. The
  lines carry no UUID, so this is not a correlation.
- This run's launch log has no exit -6 record. Its only container death record is exit -11
  (line 23 of `L1`), which is after all the quoted entries.

## E3: the run behind the reported graph-timing observation

```text
A4:L19| 1790216053.3670843 [ERROR] [component_container_isolated-5]: process has died [pid 139109, exit code -6, cmd '/opt/ros/jazzy/lib/rclcpp_components/component_container_isolated --ros-args --log-level info --ros-args -r __node:=nav2_container --params-file /tmp/launch_params_kh5nnry4 --params-file /tmp/launch_params_l23ya3uk -r /tf:=tf -r /tf_static:=tf_static'].
C1:L289| [INFO] [1790216035.606406416] [bt_navigator]: Goal succeeded
```

The report's approximate abort anchor for the graph-timing observation was 21:14:06.23. This
run's launch-log death record (`A4`) is at 21:14:13.367, 7.1 s later. The
last entry in this run's container log (`C1`, line 289, the last line of the file) is at
21:13:55.606, 17.8 s before the death record. The container log has no entry in the
interval between them. The report's other time for that observation (publisher and service gone
at 21:14:26.246) was reported from client poller output, which was not found. The retained material
therefore cannot re-establish the interval between the abort and the graph noticing it. The
approximately 20 s stays as originally reported.

## E4: the one other file that contains the phrase

```text
X1:L1| [ERROR] [1790178414.189585580] [fake_navigate_server.action_server]: Failed to accept new goal with ID [151 150  83  85 126 237  79 161 187  54 123  27  54 212 246 185]: Failed to accept new goal: feedback publisher is invalid, at ./src/rcl_action/action_server.c:1004
X1:L2| [WARN] [1790178414.190277604] [fake_navigate_server.action_server]: Failed to send goal response (the client may have gone away)
```

This is a different message from a different program at a different time. Its logger is
`fake_navigate_server.action_server`, not a Nav2 node. It was written at 10:46:54.189, before the
first launch of the evening runs (21:01:46). It reads `Failed to accept new goal with ID [...]:
Failed to accept new goal: feedback publisher is invalid, at ./src/rcl_action/action_server.c:1004`.
It is not the `terminate called ... what(): Failed to accept new goal` reported for the Nav2
container, and it is not evidence for or against the duplicate-UUID abort. Its origin was not
investigated. It is quoted only so the absence statement above is exact.

## Redactions and omissions

- Redacted, consistently: the home directory prefix would be written `<HOME>` and the host name
  `<HOST>`. Paths are written relative to the home directory (`~`). The host name appears in
  run-directory names, so each run directory reads `<start time>-<HOST>-<pid>`. Replacements
  made in source paths: 0 for the home directory,
  9 for the host name. Replacements made inside quoted log lines:
  0.
- Not redacted: process ids, epoch timestamps, `/tmp/launch_params_*` temporary file names inside
  `cmd` fields, and navigation coordinates.
- Nothing else was rewritten. Log messages and timestamps are exactly as in the source lines.
- Omitted: every other line of these files, line 1 of each launch log (it contains the home
  directory and host name), the other launch and container logs of the day, the logs of other
  nodes, all client-side material, and session transcripts. No credentials were involved.
