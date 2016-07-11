#!/bin/bash
# Script that tests the performance of inserts with and without indices
function shuf_rep() {
    if [[ -z ${1+x} ]]
    then
        echo "rows unset"
        return 1
    elif [[ -z ${2+x} ]]
    then
        echo "Min value unset"
        return 1
    elif [[ -z ${3+x} ]]
    then 
        echo "Min value unset"
        return 1
    elif [[ -z ${4+x} ]]
    then 
        echo "Destination file unset"
        return 1
    fi

    #TODO seemingly faster implementation:
    NUM_LINES=$1
    MIN_VAL=$2
    MAX_VAL=$3
    rm -f $4
    cat /dev/urandom | tr -dc 0-9 | fold -b3 | head -n$NUM_LINES | \
        sed "s/$/%($MAX_VAL-$MIN_VAL+1)+$MIN_VAL/g" |  bc > $4
#    for val in `seq 1 $NUM_LINES`
#    do
#        echo $(( RANDOM%(MAX_VAL-MIN_VAL+1) + MIN_VAL )) >> $4
#    done    
}

function wait_for_jobs() {
    FAIL=0
    for job in `jobs -p`; do
        wait $job || let "FAIL+=1"
    done
    if [ $FAIL -eq 0 ]; then
        echo Jobs done!
    else
        echo Some jobs failed
        return 1
    fi
}

function gen_data() {
    rm -f /tmp/col*
    COL1_W=10
    if [[ -z ${1+x} ]]
    then
        # Set a default value for dataset rows:
        echo "rows unset"
        return 1
    fi
    if [[ -z ${2+x} ]]
    then
        echo "Set second argument to 0 or 1"
        return 1
    fi
    NUM_LINES=$1
    echo col1
    cat /dev/urandom | tr -dc A-Za-z | fold -b$COL1_W | head -n$NUM_LINES > \
        "/tmp/col1" &
    # If you want random number of characters
#    rm -f "/tmp/col1_1"
#    while read line
#    do
#        echo $line | cut -c1-$(( RANDOM%$COL1_W+1 )) >>/tmp/col1_1
#    done < /tmp/col1
#    mv /tmp/col1_1 /tmp/col1

    # Integer column:
    LO=0
    HI=$NUM_LINES
    # TODO col2 is empty sometimes
    echo col2
    shuf -i$LO-$HI -n$NUM_LINES > /tmp/col2 &

    # Date column
    echo col3
    shuf_rep $NUM_LINES 0 59 /tmp/col3_s &
    wait_for_jobs
    shuf /tmp/col3_s > /tmp/col3_m  &
#    shuf_rep $NUM_LINES 0 59 /tmp/col3_m
    shuf_rep $NUM_LINES 0 23 /tmp/col3_h &
    shuf_rep $NUM_LINES 1 28 /tmp/col3_d & # all februaries :)
#    wait_for_jobs
    shuf_rep $NUM_LINES 1 12 /tmp/col3_m &
    shuf_rep $NUM_LINES 2014 2016 /tmp/col3_y &
    wait_for_jobs

    paste /tmp/col3_y /tmp/col3_m /tmp/col3_d /tmp/col3_h /tmp/col3_m /tmp/col3_s | \
        awk '{printf("%d-%02d-%02d %02d:%02d:%02d\n",$1,$2,$3,$4,$5,$6)}' > /tmp/col3
#    while read line
#    do
#        date -d" - $(( line )) seconds" +"%Y-%m-%d %H:%M:%S" >> /tmp/col3
#    done < /tmp/col2
    
    # Flag column (used to drop for benchmarking at a given size)
    echo col4
    tr -dc $2 < /dev/urandom | head -c$NUM_LINES | fold -1 > /tmp/col4

    # Paste columns:
    echo pasting
    DELIM=","
    paste -d$DELIM /tmp/col1 /tmp/col2 /tmp/col3 /tmp/col4 > /tmp/file_insert
}

function create_indices() {
    # Rebuild indices
    echo "Rebuilding indices"
    T0=$(date +%s)
    for IND in seq 1 3
    do
        COMMAND='CREATE INDEX "ind_col'$IND'"
         ON test_insert USING btree ("col'$IND'");'
        echo $COMMAND
        psql -d test -c "$COMMAND" 
    done
    T1=$(date +%s)
    echo elapsed: $((T1-T0)) seconds.
}


function drop_indices() {
    # Drop indices
    for IND in seq 1 3
    do
        COMMAND='DROP INDEX IF EXISTS "ind_col'$IND'"'
        echo $COMMAND
        psql -d test -c "$COMMAND"
    done
}

function insert_data() {
    # Does what it says
    if [[ -z ${1+x} ]]
    then
        echo "please say whether you want to drop indices or not"
        return 1
    elif [[ !( -z ${1+x}) && $1 -eq 1 ]]
    then
        echo "Insert dropping indices"
        drop_indices
    elif [[ ! -z ${1+x} && $1 -eq 0 ]]
    then
        echo "Insert without dropping indices"
    else
        echo "You screwed something up"
    fi
}

function reset_db() {
    # Drop table, generates a file of given size, insert and sets indices
    psql -d "postgres" -c "create database if IF NOT EXISTS test"
    psql -d "postgres" -c "drop table IF EXISTS test_insert ;" 
    psql -d "postgres" -c "create table test_insert ( col1 character varying,
    col2 int, col3 datetime);" -q
    if [[ -z ${1+x} ]]
    then
        echo "set how many lines you want for creating the db"
        return 1
    elif [[ !( -z ${1+x}) && $1 -ge 1 ]]
    then
        gen_data $1
    else 
        echo "set a numeric number of rows greater than 1"
        return 1
    fi
}
