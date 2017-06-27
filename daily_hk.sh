#!/bin/bash

date=`date "+%y%m%d"`
timestamp=`date "+%y%m%d_%H:%M:%S:%N"`
base_dir="/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in"
backup_dir=`echo "$base_dir/done_$date"`
echo $backup_dir
#mv /mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/done /mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/done200321; mkdir /mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/done

#tar -zcvf $backup_dir

echo "$timestamp: Rotated done directory $backup_dir">>/var/log/cpdiag/cpdiag_hk.log
