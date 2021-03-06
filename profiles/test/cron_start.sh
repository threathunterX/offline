#!/bin/bash

APP_NAME="nebula_offline_cron"
SHUTDOWN_WAIT=10

MAIN_CLASS="com.threathunter.bordercollie.slot.api.ServerMain"
# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
SAVED="`pwd`"
cd "`dirname \"$PRG\"`" >&-
APP_HOME="`pwd -P`"
cd "$SAVED" >&-


JAVA_OPS="
-DAPP_HOME=$APP_HOME
-Xms5g
-Xmx5g
-Xmn3g
-Xss256k
-XX:PermSize=128m
-XX:MaxPermSize=128m
-XX:MaxDirectMemorySize=2g
-XX:SurvivorRatio=8
-XX:-DisableExplicitGC
-XX:+UseConcMarkSweepGC
-XX:CMSInitiatingOccupancyFraction=70
-XX:+UseCMSCompactAtFullCollection
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+PrintHeapAtGC
-Xloggc:$APP_HOME/logs/gc.log
-XX:-OmitStackTraceInFastThrow
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp
-Dfile.encoding=utf-8
-Dsun.net.AEROSPIKE_CLIENT.defaultConnectTimeout=10000
-Dsun.net.AEROSPIKE_CLIENT.defaultReadTimeout=30000
"

_pid() {
    echo `ps aux | grep $MAIN_CLASS | grep -v grep | awk '{ print $2 }'`
    #echo `jps | grep $(expr ${MAIN_CLASS} : ".*\.\(.*\)$")| grep -v Jps | awk '{ print $1 }'`
}

start(){
    pid=$(_pid)
    if [ -n "$pid" ]
    then
         echo "$APP_NAME is already running (pid: $pid)"
    else
         #Start Program
         echo "Starting $APP_NAME"
         if [ ! -d ${APP_HOME}/logs ]
         then
             mkdir ${APP_HOME}/logs
         fi
         java ${JAVA_OPS} -Xbootclasspath/a:/home/threathunter/nebula/nebula_offline_slot/config -classpath /etc/nebula/:/etc/nebula/offline/:$(echo ${APP_HOME}/lib/* | tr ' ' ':') ${MAIN_CLASS} $ARGS 1>${APP_HOME}/logs/log.txt 2>${APP_HOME}/logs/err.txt &
        pid=$(_pid)
        if [ -n "$pid" ]
        then
            echo "$APP_NAME started: $pid"
        else
            echo "$APP_NAME failed to start"
        fi
    fi
    return 0
}

stop(){
    pid=$(_pid)
    if [ -n "$pid" ]
    then
        echo "Stoping $APP_NAME"
        kill ${pid}

        let kwait=$SHUTDOWN_WAIT
        count=0;
        until [ `ps -p ${pid} | grep -c ${pid}` = '0' ] || [ ${count} -gt ${kwait} ]
        do
            echo -n -e "\nwaiting for processes to exit";
            sleep 1
            let count=$count+1;
        done

        if [ ${count} -gt ${kwait} ]; then
            echo -n -e "\nkilling processes which didn't stop after $SHUTDOWN_WAIT seconds"
            kill -9 ${pid}
        fi
    else
        echo "$APP_NAME is not running"
    fi
    return 0
}

case $1 in
start)
    ARGS=$2
    start
    ;;
stop)
    stop
    ;;
restart)
    stop
    start
    ;;
status)
    pid=$(_pid)
    if [ -n "$pid" ]
    then
        echo "$APP_NAME is running with pid: $pid"
    else
        echo "$APP_NAME is not running"
    fi
    ;;
esac
exit 0
