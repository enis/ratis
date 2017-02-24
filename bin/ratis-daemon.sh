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
# Runs a Hadoop ratis command as a daemon.
#
# Environment Variables
#
#   RATIS_CONF_DIR   Alternate ratis conf dir. Default is ${RATIS_HOME}/conf.
#   RATIS_LOG_DIR    Where log files are stored.  PWD by default.
#   RATIS_PID_DIR    The pid files are stored. /tmp by default.
#   RATIS_IDENT_STRING   A string representing this instance of ratis. $USER by default
#   RATIS_NICENESS The scheduling priority for daemons. Defaults to 0.
#   RATIS_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        Default 1200 seconds.
#
# Modelled after hadoop-daemon.sh

usage="Usage: ratis-daemon.sh [--config <conf-dir>]\
 [--autostart-window-size <window size in hours>]\
 [--autostart-window-retry-limit <retry count limit for autostart>]\
 (start|stop|restart|autostart|autorestart|foreground_start) <ratis-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

# default autostart args value indicating infinite window size and no retry limit
AUTOSTART_WINDOW_SIZE=0
AUTOSTART_WINDOW_RETRY_LIMIT=0

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/ratis-config.sh
. "$bin"/ratis-common.sh

# get arguments
startStop=$1
shift

command=$1
shift

ratis_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanAfterRun() {
  if [ -f ${RATIS_PID} ]; then
    # If the process is still running time to tear it down.
    kill -9 `cat ${RATIS_PID}` > /dev/null 2>&1
    rm -f ${RATIS_PID} > /dev/null 2>&1
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$RATIS_PID_DIR"
    if [ -f $RATIS_PID ]; then
      if kill -0 `cat $RATIS_PID` > /dev/null 2>&1; then
        echo $command running as process `cat $RATIS_PID`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${RATIS_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$RATIS_LOG_DIR" = "" ]; then
  export RATIS_LOG_DIR="$RATIS_HOME/logs"
fi
mkdir -p "$RATIS_LOG_DIR"

if [ "$RATIS_PID_DIR" = "" ]; then
  RATIS_PID_DIR=/tmp
fi

if [ "$RATIS_IDENT_STRING" = "" ]; then
  export RATIS_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export RATIS_LOG_PREFIX=ratis-$RATIS_IDENT_STRING-$command-$HOSTNAME
export RATIS_LOGFILE=$RATIS_LOG_PREFIX.log

if [ -z "${RATIS_ROOT_LOGGER}" ]; then
export RATIS_ROOT_LOGGER=${RATIS_ROOT_LOGGER:-"INFO,RFA"}
fi

if [ -z "${RATIS_SECURITY_LOGGER}" ]; then
export RATIS_SECURITY_LOGGER=${RATIS_SECURITY_LOGGER:-"INFO,RFAS"}
fi

RATIS_LOGOUT=${RATIS_LOGOUT:-"$RATIS_LOG_DIR/$RATIS_LOG_PREFIX.out"}
RATIS_LOGGC=${RATIS_LOGGC:-"$RATIS_LOG_DIR/$RATIS_LOG_PREFIX.gc"}
RATIS_LOGLOG=${RATIS_LOGLOG:-"${RATIS_LOG_DIR}/${RATIS_LOGFILE}"}
RATIS_PID=$RATIS_PID_DIR/ratis-$RATIS_IDENT_STRING-$command.pid
export RATIS_AUTOSTART_FILE=$RATIS_PID_DIR/ratis-$RATIS_IDENT_STRING-$command.autostart

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${RATIS_LOGGC}"}
fi
if [ -n "$CLIENT_GC_OPTS" ]; then
  export CLIENT_GC_OPTS=${CLIENT_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${RATIS_LOGGC}"}
fi

# Set default scheduling priority
if [ "$RATIS_NICENESS" = "" ]; then
    export RATIS_NICENESS=0
fi

thiscmd="$bin/$(basename ${BASH_SOURCE-$0})"
args=$@

case $startStop in

(start)
    check_before_start
    ratis_rotate_log $RATIS_LOGOUT
    ratis_rotate_log $RATIS_LOGGC
    echo running $command, logging to $RATIS_LOGOUT
    $thiscmd --config "${RATIS_CONF_DIR}" \
        foreground_start $command $args < /dev/null > ${RATIS_LOGOUT} 2>&1  &
    disown -h -r
    sleep 1; head "${RATIS_LOGOUT}"
  ;;

(autostart)
    check_before_start
    ratis_rotate_log $RATIS_LOGOUT
    ratis_rotate_log $RATIS_LOGGC
    echo running $command, logging to $RATIS_LOGOUT
    nohup $thiscmd --config "${RATIS_CONF_DIR}" --autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT} \
        internal_autostart $command $args < /dev/null > ${RATIS_LOGOUT} 2>&1  &
  ;;

(autorestart)
    echo running $command, logging to $RATIS_LOGOUT
    # stop the command
    $thiscmd --config "${RATIS_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${RATIS_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi

    check_before_start
    ratis_rotate_log $RATIS_LOGOUT
    ratis_rotate_log $RATIS_LOGGC
    nohup $thiscmd --config "${RATIS_CONF_DIR}" --autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT} \
        internal_autostart $command $args < /dev/null > ${RATIS_LOGOUT} 2>&1  &
  ;;

(foreground_start)
    trap cleanAfterRun SIGHUP SIGINT SIGTERM EXIT
    if [ "$RATIS_NO_REDIRECT_LOG" != "" ]; then
        # NO REDIRECT
        echo "`date` Starting $command on `hostname`"
        echo "`ulimit -a`"
        # in case the parent shell gets the kill make sure to trap signals.
        # Only one will get called. Either the trap or the flow will go through.
        nice -n $RATIS_NICENESS "$RATIS_HOME"/bin/ratis \
            --config "${RATIS_CONF_DIR}" \
            $command "$@" start &
    else
        echo "`date` Starting $command on `hostname`" >> ${RATIS_LOGLOG}
        echo "`ulimit -a`" >> "$RATIS_LOGLOG" 2>&1
        # in case the parent shell gets the kill make sure to trap signals.
        # Only one will get called. Either the trap or the flow will go through.
        nice -n $RATIS_NICENESS "$RATIS_HOME"/bin/ratis \
            --config "${RATIS_CONF_DIR}" \
            $command "$@" start >> ${RATIS_LOGOUT} 2>&1 &
    fi
    # Add to the command log file vital stats on our environment.
    ratis_pid=$!
    echo $ratis_pid > ${RATIS_PID}
    wait $ratis_pid
  ;;

(internal_autostart)
    ONE_HOUR_IN_SECS=3600
    autostartWindowStartDate=`date +%s`
    autostartCount=0
    touch "$RATIS_AUTOSTART_FILE"

    # keep starting the command until asked to stop. Reloop on software crash
    while true
    do
      if [ -f $RATIS_PID ] &&  kill -0 "$(cat "$RATIS_PID")" > /dev/null 2>&1 ; then
        wait "$(cat "$RATIS_PID")"
      else
        #if the file does not exist it means that it was not stopped properly by the stop command
        if [ ! -f "$RATIS_AUTOSTART_FILE" ]; then
          echo "`date` Ratis might be stopped removing the autostart file. Exiting Autostart process" >> ${RATIS_LOGOUT}
          exit 1
        fi

        echo "`date` Autostarting ratis $command service. Attempt no: $(( $autostartCount + 1))" >> ${RATIS_LOGLOG}
        touch "$RATIS_AUTOSTART_FILE"
        $thiscmd --config "${RATIS_CONF_DIR}" foreground_start $command $args
        autostartCount=$(( $autostartCount + 1 ))
      fi

      curDate=`date +%s`
      autostartWindowReset=false

      # reset the auto start window size if it exceeds
      if [ $AUTOSTART_WINDOW_SIZE -gt 0 ] && [ $(( $curDate - $autostartWindowStartDate )) -gt $(( $AUTOSTART_WINDOW_SIZE * $ONE_HOUR_IN_SECS )) ]; then
        echo "Resetting Autorestart window size: $autostartWindowStartDate" >> ${RATIS_LOGOUT}
        autostartWindowStartDate=$curDate
        autostartWindowReset=true
        autostartCount=0
      fi

      # kill autostart if the retry limit is exceeded within the given window size (window size other then 0)
      if ! $autostartWindowReset && [ $AUTOSTART_WINDOW_RETRY_LIMIT -gt 0 ] && [ $autostartCount -gt $AUTOSTART_WINDOW_RETRY_LIMIT ]; then
        echo "`date` Autostart window retry limit: $AUTOSTART_WINDOW_RETRY_LIMIT exceeded for given window size: $AUTOSTART_WINDOW_SIZE hours.. Exiting..." >> ${RATIS_LOGLOG}
        rm -f "$RATIS_AUTOSTART_FILE"
        exit 1
      fi

      # wait for shutdown hook to complete
      sleep 20
    done
  ;;

(stop)
    echo running $command, logging to $RATIS_LOGOUT
    rm -f "$RATIS_AUTOSTART_FILE"
    if [ -f $RATIS_PID ]; then
      pidToKill=`cat $RATIS_PID`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $RATIS_LOGLOG
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $RATIS_PID
    fi
    rm -f $RATIS_PID
  ;;

(restart)
    echo running $command, logging to $RATIS_LOGOUT
    # stop the command
    $thiscmd --config "${RATIS_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${RATIS_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${RATIS_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
