#!/bin/bash

function timedate() {
    TZ="America/Los_Angeles" date
}

if [[ "$#" -ne 3 ]]; then
    echo "Incorrect number of arguments."
    echo "Usage is as follows:"
    echo "sh util_tablgentpcds.sh SCALE FORMAT"
    exit 1
elif [[ "$1" -lt 1 && "$1" -gt 100000 ]]; then
    echo "Invalid. Supported scale are: 2 to 100000"
    exit 1
elif [[ "$2" != "orc" && "$2" != "parquet" && "$2" != "txt" && "$2" != "external" ]]; then
    echo "Invalid. Supported formats are: orc|parquet|text|external"
    exit 1
else
    # scale ~GB
    INPUT_SCALE="$1"
    FORMAT="$2"
    GEN="$3"
fi


    # Name of clock file
    TABLE_GENERATE_LOG="table_generation_log.txt"
    # Clock file
    rm $TABLE_GENERATE_LOG
    echo "Old log removed"
    echo "Created new log"
    echo "Table gen time for TPC-DS $INPUT_SCALE" > $TABLE_GENERATE_LOG
    timedate >> $TABLE_GENERATE_LOG
    echo "" >> $TABLE_GENERATE_LOG

if [[ "$GEN" -eq 1 ]]; then
      # data generation
      echo "Start data generation" >> $TABLE_GENERATE_LOG
      timedate >> $TABLE_GENERATE_LOG
      hdfs dfs -copyFromLocal tpcds_resources /tmp
      beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsData.hql -f TPCDSDataGen.hql --hiveconf SCALE=$INPUT_SCALE --hiveconf PARTS=$INPUT_SCALE --hiveconf LOCATION=/HiveTPCDS_$INPUT_SCALE/ --hiveconf TPCDSBIN=`grep -A 1 "fs.defaultFS" /etc/hadoop/conf/core-site.xml | tail -1 | sed -e 's/.*<value>\(.*\)<\/value>.*/\1/'`/tmp/tpcds_resources
      echo "End" >> $TABLE_GENERATE_LOG
      timedate >> $TABLE_GENERATE_LOG
      echo "" >> $TABLE_GENERATE_LOG
fi


    MAX_REDUCERS=2600 # ~7 years of data hortonworks
    REDUCERS=$((test ${INPUT_SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${INPUT_SCALE})

    # table creation
    #hdfs dfs -mkdir -p /HiveTPCDS_$INPUT_SCALE/
    hdfs dfs -chmod -R 777 /HiveTPCDS_$INPUT_SCALE/
    echo "Start table generation" >> $TABLE_GENERATE_LOG
    timedate >> $TABLE_GENERATE_LOG
    beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/createAllExternalTables.hql --hiveconf LOCATION=/HiveTPCDS_$INPUT_SCALE/ --hiveconf DBNAME=tpcds_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
    echo "End" >> $TABLE_GENERATE_LOG
    timedate >> $TABLE_GENERATE_LOG
    echo "" >> $TABLE_GENERATE_LOG

    if [[ "$FORMAT" == "orc" ]]; then
        # orc tables
        echo "Start orc table generation" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/createAllORCTables.hql --hiveconf ORCDBNAME=tpcds_orc_$INPUT_SCALE --hiveconf SOURCE=tpcds_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG

        echo "Start orc analysis" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/analyze.hql --hiveconf DB=tpcds_orc_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG
    elif [[ "$FORMAT" == "parquet" ]]; then
        # parquet tables
        echo "Start parquet table generation" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/createAllParquetTables.hql --hiveconf PARQUETDBNAME=tpcds_parquet_$INPUT_SCALE --hiveconf SOURCE=tpcds_$INPUT_SCALE
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG

        echo "Start parquet analysis" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/analyze.hql --hiveconf DB=tpcds_parquet_$INPUT_SCALE
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG
    elif [[ "$FORMAT" == "text" ]]; then
        # text tables
        echo "Start text table generation" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/createAllTextTables.hql --hiveconf TEXTDBNAME=tpcds_text_$INPUT_SCALE --hiveconf SOURCE=tpcds_$INPUT_SCALE
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG

        echo "Start text analysis" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        beeline -u "jdbc:hive2://`hostname -f`:10001/;transportMode=http" -i settingsTable.hql -f tpcds_dll/analyze.hql --hiveconf DB=tpcds_text_$INPUT_SCALE
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG
    else
        echo "Start external table generation" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "End" >> $TABLE_GENERATE_LOG
        timedate >> $TABLE_GENERATE_LOG
        echo "" >> $TABLE_GENERATE_LOG
    fi

    echo "End time" >> $TABLE_GENERATE_LOG
    timedate >> $TABLE_GENERATE_LOG

