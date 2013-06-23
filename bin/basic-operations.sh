#!/usr/bin/env bash

CLASS=tachyon.examples.BasicOperations

bin_dir=`cd "$( dirname "$0" )"; pwd`

. "$bin_dir/tachyon-config.sh"
java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties -Dtachyon.logger.type=USER_LOGGER $CLASS $@
