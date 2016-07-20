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
        echo "Number of rows of gen_data unset"
        return 1
    fi
    if [[ -z ${2+x} ]]
    then
        echo "Set second argument to 0 or 1 (test column)"
        return 1
    fi
    NUM_LINES=$1
    LINE_BLOCK=100000
    NUM_REPEATS=1
    # generate data in chunks and then shuffle it
    if [[ $NUM_LINES -ge $LINE_BLOCK ]]
    then
        NUM_REPEATS=$(( NUM_LINES/LINE_BLOCK ))
        NUM_LINES=$LINE_BLOCK
    fi

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
    paste -d$DELIM /tmp/col1 /tmp/col2 /tmp/col3 /tmp/col4 > /tmp/file_insert_shuf
    # header file
    echo "col1,col2,col3,col4" > /tmp/file_insert
    # Shuffle for each split:
    echo shuffling file $NUM_REPEATS times
    for IND_SHUF in `seq 1 $NUM_REPEATS`
    do
        shuf /tmp/file_insert_shuf >> /tmp/file_insert
    done
}

function create_indices() {
    # Rebuild indices
    echo "Rebuilding indices"
    T0=$(date +%s)
    for IND_CREATE in `seq 1 3`
    do
        COMMAND='CREATE INDEX "ind_col'$IND_CREATE'"
         ON test_insert USING btree ("col'$IND_CREATE'");'
        echo $COMMAND
        psql -d test -c "$COMMAND" 
    done
    T1=$(date +%s)
    echo elapsed: $((T1-T0)) seconds.
}


function drop_indices() {
    # Drop indices
    for IND_DROP in `seq 1 3`
    do
        COMMAND='DROP INDEX IF EXISTS "ind_col'$IND_DROP'"'
        echo $COMMAND
        psql -d test -c "$COMMAND"
    done
}

function drop_test_data() {
    # Drops data with column = 1
    psql -d test -c "DELETE FROM test_insert where col4=1;"
}

function insert_data() {
    # Does what it says
    if [[ -z ${1+x} ]]
    then
        echo "please say whether you want to drop indices or not"
        return 1
    elif [[ ! -z ${1+x} && $1 -eq 1 ]]
    then
        echo "Insert dropping indices"
        drop_indices
        copy_data
        create_indices
    elif [[ ! -z ${1+x} && $1 -eq 0 ]]
    then
        echo "Insert without dropping indices"
        copy_data
    else
        echo "error in parameters of test_insert"
        return 1
    fi

    #COMMAND='\copy tmp_x FROM '/tmp/file_insert' DELIMITER ',' CSV HEADER;'
    COMMAND='\copy tmp_x FROM '/tmp/file_insert' DELIMITER ',' CSV HEADER;'
}

function copy_data() {
    if [ ! -f /tmp/file_insert ]
    then
        echo No file to insert
        return 1
    else
        COMMAND="\copy test_insert FROM '/tmp/file_insert' DELIMITER ',' CSV HEADER;"
        psql -d test -c "$COMMAND"
    fi
}

function reset_db() {
    # Drop table, generates a file of given size, insert and sets indices
    psql  -d "postgres" -c "drop database test;"
    psql  -d "postgres" -c "create database test;"
    psql -d test -c "create table test_insert ( col1 character varying,
    col2 int, col3 timestamp, col4 smallint);" -q
    create_indices
    if [[ -z ${1+x} ]]
    then
        echo "Database created but no data inserted"
        return 1
    elif [[ !( -z ${1+x}) && $1 -ge 1 ]]
    then
        gen_data $1
    else 
        echo "set a numeric number of rows greater than 1"
        return 1
    fi
}

function benchmark() {
    # Low value, high value, and increment
    LO_SIZE=100000
    HI_SIZE=1000000
    IN_SIZE=100000
    LO_INITIAL=10000000
    HI_INITIAL=10000000
    IN_INITIAL=10000000
    # File with results:
    echo "initial_size,size,index,time" > /tmp/results
    for INITIAL_SIZE in `seq $LO_INITIAL $IN_INITIAL $HI_INITIAL`
    do
    # Leave it clean
    reset_db
    # Gen initial data
    gen_data $INITIAL_SIZE 0
    # Insert it (no matter much since the table is empty)
    insert_data 1
    for IND_INDEX in `seq 0 1`
    do
    for SIZE_INS in `seq $LO_SIZE $IN_SIZE $HI_SIZE`
    do 
        echo 'testing with IND '$IND_INDEX', and '$SIZE_INS' rows'
        # produce some random test data
        gen_data $SIZE_INS 1
        T0=$(date +%s);
        insert_data $IND_INDEX
        T1=$(date +%s);

        # remove the inserted data
        drop_test_data

        # Vacuum table to leave into the original stage
        psql -d test -c "VACUUM test_insert;"
        echo "$INITIAL_SIZE,$SIZE_INS, $IND_INDEX, $(( T1-T0 ))" >> /tmp/results
    done
    done
    done
}
