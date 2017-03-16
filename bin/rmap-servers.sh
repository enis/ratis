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
# Run a shell command on all rmapserver hosts.
#
# Environment Variables
#
#   RATIS_RMAPSERVERS    File naming remote hosts.
#     Default is ${RATIS_CONF_DIR}/rmapservers
#   RATIS_CONF_DIR  Alternate ratis conf dir. Default is ${RATIS_HOME}/conf.
#   RATIS_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   RATIS_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: rmapservers [--config <ratis-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/ratis-config.sh

# If the rmapservers file is specified in the command line,
# then it takes precedence over the definition in 
# ratis-env.sh. Save it here.
HOSTLIST=$RATIS_RMAPSERVERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$RATIS_RMAPSERVERS" = "" ]; then
    export HOSTLIST="${RATIS_CONF_DIR}/rmapservers"
  else
    export HOSTLIST="${RATIS_RMAPSERVERS}"
  fi
fi

rmapservers=`cat "$HOSTLIST"`
if [ "$rmapservers" = "localhost" ]; then
  RATIS_RMAPSERVERSSERVER_ARGS="\
    -Dratis.rmapserver.port=16201 \
    -Dratis.rmapserver.info.port=16301"

  $"${@// /\\ }" ${RATIS_RMAPSERVER_ARGS} \
        2>&1 | sed "s/^/$rmapserver: /" &
else
  for rmapserver in `cat "$HOSTLIST"`; do
    if ${RATIS_SLAVE_PARALLEL:-true}; then
      ssh $RATIS_SSH_OPTS $rmapserver $"${@// /\\ }" \
        2>&1 | sed "s/^/$rmapserver: /" &
    else # run each command serially
      ssh $RATIS_SSH_OPTS $rmapserver $"${@// /\\ }" \
        2>&1 | sed "s/^/$rmapserver: /"
    fi
    if [ "$RATIS_SLAVE_SLEEP" != "" ]; then
      sleep $RATIS_SLAVE_SLEEP
    fi
  done
fi

wait
