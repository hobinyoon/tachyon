#!/usr/bin/env bash

CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# HDFS
#export TACHYON_UNDERFS_ADDRESS=hdfs://localhost:9000

# Local
export TACHYON_UNDERFS_ADDRESS=/home/hobin/work/tachyon

export TACHYON_JAVA_OPTS="
  -Dtachyon.debug=true
  -Dtachyon.worker.timeout.ms=60000
  -Dtachyon.underfs.address=$TACHYON_UNDERFS_ADDRESS
  -Dtachyon.worker.memory.size=1024MB
  -Dtachyon.worker.data.folder=/mnt/ramdisk/tachyonworker/
  -Dtachyon.master.hostname=localhost
  -Dtachyon.master.log.file=$TACHYON_UNDERFS_ADDRESS/tachyon/tachyon_log.data
  -Dtachyon.master.checkpoint.file=$TACHYON_UNDERFS_ADDRESS/tachyon/tachyon_checkpoint.data
  -Dtachyon.master.pinlist=/pinfiles;/pindata
"
