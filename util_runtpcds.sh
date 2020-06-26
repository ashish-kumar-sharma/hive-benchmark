#!/bin/bash

function timedate() {
    TZ="America/Los_Angeles" date
}

ID=`TZ="America/Los_Angeles" date +"%m.%d.%Y-%H.%M.%S"`

if [[ "$#" -ne 3 ]]; then
    echo "Incorrect number of arguments."
    echo "Usage is as follows:"
    echo "sh util_runtpcds.sh SCALE FORMAT PARALLELISM "
    exit 1
fi

if [[ "$1" -le 1 && "$1" -gt 100000 ]]; then
    echo "Invalid. Supported scale are: 2 to 100000"
    exit 1
elif [[ "$2" != "orc" && "$2" != "parquet" && "$2" != "txt" && "$2" != "external" ]]; then
    echo "Invalid. Supported formats are: orc|parquet|text|external"
    exit 1
elif [[ "$3" -ne 0 && "$3" -ne 1 ]]; then
    echo "Invalid. Supported formats 0 - disable PARALLELISM and 1 - enable PARALLELISM"
    exit 1
else
    # scale ~GB
    SCALE="$1"
    PARALLELISM="$3"
    FORMAT="$2"
fi

    # query file name
    QUERY_BASE_NAME="tpcds_queries/tpcds_query"
    QUERY_FILE_EXT=".sql"
    # settings file location
    SETTINGS_PATH="settings.hql"

    # report name
    REPORT_NAME="time_elapsed_tpcds"
    # database name
    if [[ $FORMAT == "orc" ]]; then
        DATABASE="tpcds_orc_"$SCALE
    elif [[ $FORMAT == "parquet" ]]; then
        DATABASE="tpcds_parquet_"$SCALE
    elif [[ $FORMAT == "text" ]]; then
        DATABASE="tpcds_text_"$SCALE
    else
        DATABASE="tpcds_"$SCALE
    fi

    echo "databsae - $DATABASE"
    # hostname
    HOSTNAME=`hostname -f`
    # Clock file
    CLOCK_FILE="aaa_clocktime.txt"
    rm $CLOCK_FILE
    echo "Old clock removed"
    echo "Created new clock"
    echo "Run queries for TPC-DS at scale $SCALE" > $CLOCK_FILE
    timedate >> $CLOCK_FILE

    # generate time report
    rm $REPORT_NAME*".csv"
    echo "Old report removed"
    echo "query #", "secs elapsed", "status" > $REPORT_NAME".csv"
    echo "New report generated"

    # remove old llapio_summary
    rm "llapio_summary"*".csv"
    echo "Old llapio_summary*.csv removed"

    # remove old time_precise_
    rm "time_precise_"*".csv"
    echo "Old time_precise_*.csv removed"

    # clear and make new log directory
    rm -r log_query/
    echo "Old logs removed"
    mkdir log_query/
    echo "Log folder generated"

    # make executable
    chmod +x *".sh"
    chmod -R +x PAT/

    # absolute path
    CURR_DIR="`pwd`/"

    # range of queries
    START=1
    END=99
    for (( i = $START; i <= $END; i++ )); do
        query_path=($QUERY_BASE_NAME$i$QUERY_FILE_EXT)
        LOG_PATH="log_query/logquery$i.txt"

        if [[ -f $query_path ]]; then
                if [ "$PARALLELISM" -eq 1 ]; then
                        nohup ./util_internalRunQuery.sh "$DATABASE" "$CURR_DIR$SETTINGS_PATH" "$CURR_DIR$query_path" "$CURR_DIR$LOG_PATH" "$i" "$CURR_DIR$REPORT_NAME.csv" &
                else
                        ./util_internalRunQuery.sh "$DATABASE" "$CURR_DIR$SETTINGS_PATH" "$CURR_DIR$query_path" "$CURR_DIR$LOG_PATH" "$i" "$CURR_DIR$REPORT_NAME.csv"
                fi
            # See util_internalGetPAT
            # ./util_internalGetPAT.sh /$CURR_DIR/util_internalRunQuery.sh "$DATABASE" "$CURR_DIR$SETTINGS_PATH" "$CURR_DIR$query_path" "$CURR_DIR$LOG_PATH" "$i" "$CURR_DIR$REPORT_NAME.csv" tpcdsPAT"$ID"/query"$i"/
        else
            # report failure
            echo $i, " ", "FAILURE_FILENOTFOUND" >> "$CURR_DIR$REPORT_NAME.csv"
            echo "query$i: FAILURE"
        fi
    done

    echo "End" >> $CLOCK_FILE
    timedate >> $CLOCK_FILE

    # python3 parselog.py
    # python3 parse_precisetime.py tpcds
    mv $REPORT_NAME".csv" $REPORT_NAME$ID".csv"
    zip -j log_query.zip log_query/*
    zip -r "tpcds-"$SCALE"GB-"$ID".zip" log_query.zip PAT/PAT-collecting-data/results/tpcdsPAT"$ID"/* $REPORT_NAME$ID".csv" "llapio_summary"*".csv" "time_precise_tpcds"*".csv"
    rm log_query.zip
