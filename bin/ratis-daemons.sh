#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#
# Run a ratis command on all slave hosts.
# Modelled after $HADOOP_HOME/bin/hadoop-daemons.sh

usage="Usage: ratis-daemons.sh [--config <ratis-confdir>] [--autostart-window-size <window size in hours>]\
      [--autostart-window-retry-limit <retry count limit for autostart>] \
      [--hosts serversfile] [autostart|autorestart|restart|start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

# default autostart args value indicating infinite window size and no retry limit
AUTOSTART_WINDOW_SIZE=0
AUTOSTART_WINDOW_RETRY_LIMIT=0

. $bin/ratis-config.sh

if [[ "$1" = "autostart" || "$1" = "autorestart" ]]
then
  autostart_args="--autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT}"
fi

remote_cmd="$bin/ratis-daemon.sh --config ${RATIS_CONF_DIR} ${autostart_args} $@"
args="--hosts ${RATIS_SERVERS} --config ${RATIS_CONF_DIR} $remote_cmd"

command=$2
case $command in
  # Add specific server types here
  (*)
    exec "$bin/ratis-servers.sh" $args
    ;;
esac
