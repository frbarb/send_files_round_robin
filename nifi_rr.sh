#!/bin/bash

#######################################################
# Transfer archived iglpush.bin files to NiFi Servers #
#-----------------------------------------------------#
# This script must be scheduled on the crontab as the #
# same as the archiving script below:                 #
# * * * * *       /home/oss/Live/archive_data_dump.sh #
#######################################################

#{{{ Variables
yymmddhh=$(date +%Y/%m/%d/%H/)
PDCDump_Path="/opt/intersec/archives/PDC_Dumps"
NiFi_Path="/home/intersec/NiFiTransfer"
Hourly_Path="$PDCDump_Path/$yymmddhh"
day_date=$(date +"%Y%m%d")
logfile="$NiFi_Path/logs/toNiFi-${day_date}.log"
pidfile="$NiFi_Path/toNiFi_data_dump.pid"

fileItemArray=($fileItemString)
iplist_str=`cat $NiFi_Path/NiFiServers.txt | tr "\n" " "`
iplist=($iplist_str)
last_file=$NiFi_Path/last_file.txt
rr_file=$NiFi_Path/rr_file.txt
latest_file=$NiFi_Path/latest_file.txt

#}}}

# Round-Robin sftp function
#{{{ SFTP Function
sendRR() {
log "Sending files in Round-Robin Order."
log "Sending $1 to NiFi."
rr=`cat $rr_file`

# Test echoes to be removed
echo "NB_FILES ="$NB_FILES
echo "current_hour_iglpush_archived ="$current_hour_iglpush_archived
echo "###################"
echo "Round-Robin IP = "${iplist[(($rr % 3))]}
echo "Round-Robin Index = "$rr
echo "echo dolar 1 = "$1
echo "last_file ="`cat $last_file`

grep $1 $last_file
  if [ $? -ne 0 ]; then
    # echo "# FILE SENT ="$1
	ping -c1 ${iplist[(($rr % 3))]
	if [ $? -ne 0 ]; then
	  rr=$(( rr +1 ))
      echo $rr > $rr_file
	  sendRR $1
      return (($rr % 3))
	fi
    #[ -s  $1 ] && sshpass -p 't3lus20!7' sftp nifiadmin@${iplist[(($rr % 3))]} << !
    #cd /apps/pdcdata
    #put $1
    #bye
    #!
    echo $1 > $last_file
    rr=$(( rr +1 ))
    echo $rr > $rr_file
  fi
return 171
}
#}}}


# {{{ Log Function
log() {
    local msg="$1"

    [ -z "$logfile" ] && return 0

    echo "[$(date +"%Y-%m-%d %H:%M:%S")] - $msg" >> $logfile
}
# }}}

if [ -f $pidfile ]; then
    PID=$(cat $pidfile)
    ps -p $PID > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        log "pidfile exists. Script can not run multiple times."
        exit 1
    else
        echo $$ > $pidfile
		# $$ is the process ID (PID) of the script itself
        if [ $? -ne 0 ]; then
            log "Could not create PID file."
            exit 1
        fi
    fi
else
    echo $$ > $pidfile
    if [ $? -ne 0 ]; then
        log "Could not create PID file."
        exit 1
    fi
fi

#ls -f $PDCDump_Path/*.bin
FILES="$(find $PDCDump_Path/`date +%Y/%m/%d/%H --date "30 seconds ago"` -maxdepth 1 -type f | awk -F "/" '{ print $NF }'| sort )"
NB_FILES="$(find $PDCDump_Path/`date +%Y/%m/%d/%H --date "30 seconds ago"` -maxdepth 1 -type f | awk -F "/" '{ print $NF }'| sort | wc -l)"

log "There are $NB_FILES files to send."

#Loop to iterate through last iglpush archived batch
for current_hour_iglpush_archived in $( find $PDCDump_Path/`date +%Y/%m/%d/%H --date "30 seconds ago"` -maxdepth 1 -type f | awk -F "/" '{ print $NF }'| sort )
do

sendRR $current_hour_iglpush_archived
if [ $? -ne 171 ]; then
  log "Server ${iplist[$?]} unreachable"
  echo "Server ${iplist[$?]} unreachable"  
fi

done

log "Archiving done."

rm $pidfile