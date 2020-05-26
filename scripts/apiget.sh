#!/bin/bash

# current_date=`(date '+%C%y%m%d' -d "$start_date+${date_add} days")`
# day=`(date '+%d' -d "$start_date+${date_add} days")`
# month=`(date '+%m' -d "$start_date+${date_add} days")`
# year=`(date '+%C%y' -d "$start_date+${date_add} days")`
# file_suffix=$year$month$day

state=$1

#GET http://coronavirusapi.com/getTimeSeries/$state >> ./data/State_ts_${state}_${file_suffix}.csv
rm ./data/State_ts_${state}.csv

GET http://coronavirusapi.com/getTimeSeries/$state >> ./data/State_ts_${state}.csv