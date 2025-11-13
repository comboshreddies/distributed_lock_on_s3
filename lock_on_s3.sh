#!/bin/bash
set -Euo pipefail

## example lock_on_s3.conf
#  BUCKET=your_bucket_name_here
#  S3_LOCK_PATH=locking/deploy
#  S3_WAIT_PATH=waiting/deploy
#  S3_LOCK_ACHIEVED_PATH=locktime/deploy
#
#  MAX_LOCK_DURATION_SEC=3600
#  LOCK_STALE_SEC=300
#  LOCK_LOOP_SLEEP_SEC=30
#  LOCK_VERIFY_DELAY_SEC=3
#  KEEPALIVE_LOOP_SLEEP_SEC=60
#  MAX_RETRIES=60
#  GRACEFUL_SHUTDOWN_TIMEOUT_SEC=10
#  EXP_BACKOFF_COEF=1.01
#  WAIT_STALE_TIME_SEC=300

function ts {
    echo "$(date -u --iso-8601=seconds) : $$ : ${TASK_NAME} : ${LOCK_SUFFIX}"
}

function cleanup {
    rm -rf "${TMP_DIR}"
    aws s3api delete-object --bucket "${BUCKET}" --key "${S3_WAIT_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}" 2>/dev/null || true
    # intentionally leaving lock achieved for keeping track of lock files
    # aws s3api delete-object --bucket "${BUCKET}" --key "${S3_LOCK_ACHIEVED_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}" 2>/dev/null || true
}

function print_help() {
    echo "${BNAME} : lock on s3 bucket"
    echo
    echo "syntax: "
    echo " ${BNAME} <1st_arg> <2st_arg> [ <3rd_arg> [ <4th_arg> ] ]"
    echo " or "
    echo " ${BNAME} <1st_arg> unlock <4th_arg>"
    echo
    echo " 1st arg : conf file"
    echo " 2nd arg : executable that should be executed under lock"
    echo " 3rd arg : task name, as seen on lock file (optional)"
    echo " 4th arg : lock_suffix on lock path, used for multistage locking (optional)"
    echo
    echo " check s3_lock.conf configuration file"
    echo " there you can setup bucket, lock path, timeouts"
    echo " you have example conf file in this script comment"
}

function remove_lock() {
    echo "$(ts) : NOTE: you should not be doing this, you might break lock isolation"
    echo "$(ts) : on bucket ${BUCKET}, removing ${S3_LOCK_PATH}${LOCK_SUFFIX}"
    aws s3api delete-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}"
}

function check_wait_queue() {
    echo "$(ts) : wait_queue_check : start "
    WAIT_DIR="$(dirname ${S3_WAIT_PATH})/"
    WAIT_FILE_PREFIX="$(basename ${S3_WAIT_PATH}${LOCK_SUFFIX})"
    #  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix waiting/
    aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "$WAIT_DIR"  --output json > ${TMP_DIR}/wait_list.$$.json
    AWS_LIST_OBJ_EC=$?
    if [ "${AWS_LIST_OBJ_EC}" != "0" ] ; then
        echo "$(ts) : wait_queue_check : list objects failed, will retry"
        return 0 
    fi
    WL_COUNT=$(jq '((.Contents // []) | length)'  ${TMP_DIR}/wait_list.$$.json)
    if [ "${WL_COUNT}" -eq 0 ] ;then
        return 0
    fi
    WAIT_LOOKUP_TIME=$(date -u --iso-8601=seconds -d "$WAIT_STALE_TIME_SEC seconds ago")
    jq '[.Contents[]|select(.LastModified >= "'"$WAIT_LOOKUP_TIME"'")]' ${TMP_DIR}/wait_list.$$.json > ${TMP_DIR}/wait_time_filter_list.$$.json
    jq -r '[ .[] | select(.Key | test("^'"$WAIT_FILE_PREFIX"'_[0-9]*_'"$TASK_NAME"'$","g"))]' ${TMP_DIR}/wait_time_filter_list.$$.json > ${TMP_DIR}/wait_name_filter_list.$$.json
    jq 'sort_by(.LastModified)[-5:]' ${TMP_DIR}/wait_name_filter_list.$$.json >  ${TMP_DIR}/wait_name_filter_list_5.$$.json # check last 5
    WAIT_LIST=$(jq -r '.[].Key' ${TMP_DIR}/wait_name_filter_list_5.$$.json)
    while IFS= read -r item; do
        echo "$(ts) : wait_queue_check : ITEM: $item"
        aws s3api get-object --bucket "${BUCKET}" --key "${item}" ${TMP_DIR}/freshed_wait.$$.txt > ${TMP_DIR}/freshed_wait.$$.meta.txt
        #2025-11-05T16:31:10+00:00 2025-11-05T16:31:43+00:00 : 89376 : sample :  89376
        UTC_NANO=$(cat ${TMP_DIR}/freshed_wait.$$.txt | head -1 | awk '{ print $1 }')
        PID=$(cat  ${TMP_DIR}/freshed_wait.$$.txt | head -1 | awk '{ print $2 }')
        if [ "$PID" == "$$" ] ; then
            echo "$(ts) : wait_queue_check : our lock file ${item} pid ${PID} - ignoring"
    else
            if [ ${UTC_NANO} -gt ${MAIN_START_TIME_NANO} ] ; then
                echo "$(ts) : wait_queue_check : there is a fresher wait, our ${MAIN_START_TIME_NANO} $$, other ${UTC_NANO} ${PID}"
        echo -n "$(ts) : wait_queue_check : "
        cat ${TMP_DIR}/freshed_wait.$$.txt | head -1
                return 1
            fi
        fi
    done  <<< "$WAIT_LIST"
    return 0
}

function get_lock() {
    RETRY_COUNT=0
    LOCK_WAIT_START=$(date -u +%s)
    while true ; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ "${RETRY_COUNT}" -gt "${MAX_RETRIES}" ] ; then
            echo  "$(ts) : get_lock : max retries ${MAX_RETRIES} reached (${RETRY_COUNT}), exiting"
            exit 40
        fi
        #1 check if there is a lock file
        echo "$(ts) : get_lock : =========================== retry ${RETRY_COUNT}"
        echo "$(ts) : get_lock : checking remote lock file, stale seconds ${LOCK_STALE_SEC}, pid $$"
        aws s3api head-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --output json > ${TMP_DIR}/headobj.$$.json 2> ${TMP_DIR}/headobj.$$.error
        HEADOBJ_ERROR_CODE=$?
        if [ ! -f "${TMP_DIR}/headobj.$$.error" ]; then
            echo "$(ts) : get_lock : error: expected error file not found"
            exit 41
        fi
        HEADOBJ_ERROR=$(cat ${TMP_DIR}/headobj.$$.error | grep -v ^$ || true)
        if [ "${HEADOBJ_ERROR_CODE}" -ne 0 ] || [ "${HEADOBJ_ERROR}" ] ;then
            HEADOBJ_HTTP_CODE=$(echo "${HEADOBJ_ERROR}" | sed 's/.*(\([0-9]*\)).*/\1/g' 2>/dev/null || echo "0")
            HEADOBJ_REASON=$(echo "${HEADOBJ_ERROR}" | awk -F: '{print $NF}')
            if [ "${HEADOBJ_HTTP_CODE}" == "404" ] && [ "${HEADOBJ_REASON}" == " Not Found" ] ; then
                echo "$(ts) : get_lock : lock file not found, trying to lock"
                echo "$(ts) $$" > ${TMP_DIR}/putobj_lock_file.$$
                aws s3api put-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --if-none-match '*' --metadata "{ \"ts\" : \"$(date -u +%s)\", \"pid\" : \"$$\"}" --content-type "text/plain" --body ${TMP_DIR}/putobj_lock_file.$$ > ${TMP_DIR}/putobj.$$.json 2> ${TMP_DIR}/putobj.$$.error
                PUTOBJ_ERROR_CODE=$?
                if [ ! -f "${TMP_DIR}/putobj.$$.error" ]; then
                    echo "$(ts) : get_lock : error: expected error file not found"
                    exit 42
                fi
                PUTOBJ_ERROR=$(cat ${TMP_DIR}/putobj.$$.error | grep -v ^$ || true)
                if [ "${PUTOBJ_ERROR_CODE}" -ne 0 ] || [ "${PUTOBJ_ERROR}" ] ; then
                    # put object failed, sleep and continue
                    echo "$(ts) : get_lock : error obtaining lock, leaving trace of waiting"
                    echo -n "$(ts) : get_lock : "
                    cat ${TMP_DIR}/putobj.$$.error | grep -v ^$
                    echo "$(ts) : get_lock : retry in ${LOCK_LOOP_SLEEP_SEC}"
                    continue
                else
                    # put object was successful, lock is on, notify and break
                    ADJUSTED_SLEEP=$(echo "${LOCK_LOOP_SLEEP_SEC}*(${EXP_BACKOFF_COEF}^${RETRY_COUNT})"|bc)
                    echo "$(ts) : get_lock : check if lock is stabile, sleep for ${ADJUSTED_SLEEP} and verify"
                    sleep "${ADJUSTED_SLEEP}"
                    aws s3api head-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --output json > ${TMP_DIR}/verify.$$.json 2> ${TMP_DIR}/verify.$$.error
                    VERIFY_HEAD_ERROR_CODE=$?
                    if [ "${VERIFY_HEAD_ERROR_CODE}" -ne 0 ]; then
                        echo "$(ts) : get_lock : error verifying lock ownership, retrying"
                        continue
                    fi
                    ETAG_OUTPUT=$(jq -r '.ETag // "error1"' ${TMP_DIR}/putobj.$$.json 2>/dev/null | tr -d '"' || echo "error1")
                    VERIFY_ETAG=$(jq -r '.ETag // "error2"' ${TMP_DIR}/verify.$$.json 2>/dev/null | tr -d '"' || echo "error2")
                    if [ "${VERIFY_ETAG}" == "error2" ] || [ "${ETAG_OUTPUT}" == "error1" ] || [ -z "${VERIFY_ETAG}" ] || [ -z "${ETAG_OUTPUT}" ] ; then
                        echo "$(ts) : get_lock : lock verification failed (ETag extraction error), retrying"
                        continue
                    fi
            set +x
                    echo "$(ts) : get_lock : adding informative lock achieved file ${S3_LOCK_ACHIEVED_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}"
                    cp ${TMP_DIR}/putobj_lock_file.$$ ${TMP_DIR}/putobj_lock_file.$$.lock_taken
                    echo "$(ts) $$ ${TASK_NAME} ${PROTECTED_CMD}" > ${TMP_DIR}/lock_achieved.$$.file
            aws s3api put-object --bucket "${BUCKET}" --key "${S3_LOCK_ACHIEVED_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}" --metadata "{ \"ts\" : \"$(date -u +%s)\", \"pid\" : \"$$\", \"hostname\" : \"$(hostname)\" }" --content-type "text/plain" --body ${TMP_DIR}/lock_achieved.$$.file > ${TMP_DIR}/lock_achieved.$$.json 2>/dev/null || true
                    aws s3api delete-object --bucket "${BUCKET}" --key "${S3_WAIT_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}" > ${TMP_DIR}/wdelobj.$$.json 2>/dev/null || true
                    break
                fi
            else
                echo "$(ts) : get_lock : unhandled error, exiting"
                echo "$(ts) : get_lock : error code: ${HEADOBJ_ERROR_CODE}"
                echo "$(ts) : get_lock : ${HEADOBJ_ERROR}"
                exit 43
            fi
        fi

        unset PUTOBJ_ERROR_CODE PUTOBJ_ERROR HEADOBJ_ERROR_CODE HEADOBJ_ERROR

        echo "$(ts) : get_lock : remote lock file heads fetched"
        STALE_TIME=$(date -u --iso-8601=seconds -d "${LOCK_STALE_SEC} seconds ago")
        LAST_MOD=$(jq -r '.LastModified // "error"' ${TMP_DIR}/headobj.$$.json)
        if [ "${LAST_MOD}" == "error" ] || [ -z "${LAST_MOD}" ]; then
            ADJUSTED_SLEEP=$(echo "${LOCK_LOOP_SLEEP_SEC}*(${EXP_BACKOFF_COEF}^${RETRY_COUNT})"|bc)
            echo "$(ts) : get_lock : error extracting LastModified, retrying - sleep ${ADJUSTED_SLEEP} sec)"
            sleep "${ADJUSTED_SLEEP}"
            continue
        fi
        if [[ "${LAST_MOD}" > "${STALE_TIME}" ]]; then
            echo "$(ts) : get_lock : found, not stale, _____ LAST_MOD ${LAST_MOD}"
            echo "$(ts) : get_lock : current _____________ STALE_TIME ${STALE_TIME}"
            echo -n "$(ts) : get_lock : "
            jq -r '"lm: \(.LastModified) et: \(.ETag) "' ${TMP_DIR}/headobj.$$.json
            echo -n "$(ts) : get_lock : "
            jq -r '"Metadata: ts: \(.Metadata.ts) pid: \(.Metadata.pid)"' ${TMP_DIR}/headobj.$$.json
            ADJUSTED_SLEEP=$(echo "${LOCK_LOOP_SLEEP_SEC}*(${EXP_BACKOFF_COEF}^${RETRY_COUNT})"|bc)
            echo "$(ts) : get_lock : leaving trace of waiting"
            echo "${MAIN_START_TIME_NANO} $$ ${MAIN_START_TIME} $(hostname) $(ts)" > ${TMP_DIR}/rputobj.$$.file
            aws s3api put-object --bucket "${BUCKET}" --key "${S3_WAIT_PATH}${LOCK_SUFFIX}_$$_${TASK_NAME}" --metadata "{ \"ts\" : \"$(date -u +%s)\", \"pid\" : \"$$\"}" --content-type "text/plain" --body ${TMP_DIR}/rputobj.$$.file > ${TMP_DIR}/rputobj.$$.json 2>/dev/null || true
            echo "$(ts) : get_lock : checking wait queue"
            if [ "${WAIT_STALE_TIME_SEC}" != "0" ] ; then
                check_wait_queue
                CWQ_RC=$?
                echo $CWQ_RC
                if [ $CWQ_RC -ne 0 ] ; then
                    echo "$(ts) : get_lock : wait queue failed ${CWQ_RC}, exiting"
                    exit 44
                fi
            fi
            echo "$(ts) : get_lock : sleeping for ${ADJUSTED_SLEEP} seconds, then retry (leaving trace of waiting)"
            sleep "${ADJUSTED_SLEEP}"
            continue
        else
            echo "$(ts) : get_lock : found, it is stale, LAST_MOD ${LAST_MOD} STALE_TIME ${STALE_TIME}"
            ETAG=$( jq -r '.ETag // "error"' ${TMP_DIR}/headobj.$$.json | tr -d '"' )
            if [ "${ETAG}" == "error" ] || [ -z "${ETAG}" ]; then
                echo "$(ts) : get_lock : error extracting ETag, retrying"
                ADJUSTED_SLEEP=$(echo "${LOCK_LOOP_SLEEP_SEC}*(${EXP_BACKOFF_COEF}^${RETRY_COUNT})"|bc)
                sleep "${ADJUSTED_SLEEP}"
                continue
            fi
            aws s3api delete-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --if-match "${ETAG}" > ${TMP_DIR}/do.$$.json 2> ${TMP_DIR}/do.$$.error
            DELETEOBJ_ERROR_CODE=$?
            if [ "${DELETEOBJ_ERROR_CODE}" -ne 0 ] ; then
                if [ ! -f "${TMP_DIR}/do.$$.error" ]; then
                    echo "$(ts) : get_lock : error: expected error file (for deletion) not found"
                    exit 45
                fi
                DELETEOBJ_ERROR=$(cat ${TMP_DIR}/do.$$.error | grep -v ^$ || true)
                DELETEOBJ_HTTP_CODE=$(echo "${DELETEOBJ_ERROR}" | sed 's/.*(\([0-9]*\)).*/\1/g' 2>/dev/null || echo "0")
                if [ "${DELETEOBJ_HTTP_CODE}" == "404" ]; then
                    echo "$(ts) : get_lock : stale lock already deleted by another process, retrying"
                    continue  # Lock already gone, retry acquisition
                fi
                if [ "${DELETEOBJ_ERROR_CODE}" -ne 0 ] || [ "${DELETEOBJ_ERROR}" ] ; then
                    echo "$(ts) : get_lock : error deleting lock file, error code ${DELETEOBJ_ERROR_CODE}"
                    echo "$(ts) : get_lock : ${DELETEOBJ_ERROR}"
                    echo -n "$(ts) : get_lock : "
                    cat ${TMP_DIR}/do.$$.error | grep -v ^$
                    ADJUSTED_SLEEP=$(echo "${LOCK_LOOP_SLEEP_SEC}*(${EXP_BACKOFF_COEF}^${RETRY_COUNT})"|bc)
                    echo "$(ts) : get_lock : sleep for ${ADJUSTED_SLEEP} and then retry"
                    sleep "${ADJUSTED_SLEEP}"
                fi
            else
                echo "$(ts) : get_lock : lockfile removing complete"
            fi
            unset DELETEOBJ_ERROR_CODE DELETEOBJ_ERROR DELETEOBJ_HTTP_CODE
        fi
        unset STALE_TIME LAST_MOD
    done
}


function delete_lock_file() {
    # deletion on successful completion
    # get last etag from keep alive process
    ETAG=$(jq -r '.ETag // "error"' ${TMP_DIR}/ka_putobj.$$.json | tr -d '"')
    if [ "${ETAG}" == "error" ] || [ -z "${ETAG}" ]; then
        echo "$(ts) : delete_lock : error extracting ETag"
        exit 50
    fi
    echo "$(ts) : delete_lock : removing lockfile with spotted ETAG ${ETAG}"
    aws s3api delete-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --if-match "${ETAG}" > ${TMP_DIR}/final_do.$$.json 2> ${TMP_DIR}/final_do.$$.error
    DELETEOBJ_ERROR_CODE=$?
    if [ ! -f "${TMP_DIR}/final_do.$$.error" ]; then
            echo "$(ts) : delete_lock : error: expected error file not found"
            exit 51
    fi
    DELETEOBJ_ERROR=$(cat ${TMP_DIR}/final_do.$$.error | grep -v ^$ || true)
    if [ "${DELETEOBJ_ERROR_CODE}" -ne 0 ] || [ "${DELETEOBJ_ERROR}" ] ; then
        echo "$(ts) : delete_lock : error deleting lock file, error code ${DELETEOBJ_ERROR_CODE}"
        echo "$(ts) : delete_lock : ${DELETEOBJ_ERROR}"
        echo -n "$(ts) : delete_lock : "
        cat ${TMP_DIR}/final_do.$$.error | grep -v ^$
        exit 52
    fi
    echo "$(ts) : delete_lock : lockfile removing complete"
}

function keep_alive_lock() {
    KAL_PID=${BASHPID}
    if [ ! -f "${TMP_DIR}/putobj.$$.json" ]; then
        echo "$(ts) : keepLockAlive ${KAL_PID} : error: putobj file not found"
        exit 60
    fi
    cp ${TMP_DIR}/putobj.$$.json ${TMP_DIR}/ka_putobj.$$.json
    LOCK_START_TIME=$(jq -r '.Metadata.ts // "empty"' ${TMP_DIR}/putobj.$$.json 2>/dev/null)
    if [ "${LOCK_START_TIME}" == "empty" ]; then
        # Fallback: use current time if metadata not available
        LOCK_START_TIME=$(date -u +%s)
    fi

    KEEPALIVE_MAX_RETRY=3
    KEEPALIVE_RETRY=0
    while true; do
        echo "$(ts) : keepLockAlive ${KAL_PID} : =-=-=-=-=-=-=-=-=-="
        CURRENT_TIME=$(date -u +%s)
        LOCK_AGE=$((CURRENT_TIME - LOCK_START_TIME))
        if [ "${LOCK_AGE}" -gt "${MAX_LOCK_DURATION_SEC}" ]; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : lock exceeded max duration (${LOCK_AGE}s > ${MAX_LOCK_DURATION_SEC}s), releasing"
            exit 61
        fi
        echo "$(ts) : keepLockAlive ${KAL_PID} : sleeping for ${KEEPALIVE_LOOP_SLEEP_SEC} seconds"
        sleep "${KEEPALIVE_LOOP_SLEEP_SEC}"
        echo "$(ts) : keepLockAlive ${KAL_PID} : checking remote lock file, stale seconds ${LOCK_STALE_SEC}, pid $$"
        aws s3api head-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --output json > ${TMP_DIR}/ka_headobj.$$.json 2> ${TMP_DIR}/ka_headobj.$$.error
        HEADOBJ_ERROR_CODE=$?
        if [ ! -f "${TMP_DIR}/ka_headobj.$$.error" ]; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : error: expected error file not found"
            exit 62
        fi
        HEADOBJ_ERROR=$(cat ${TMP_DIR}/ka_headobj.$$.error | grep -v ^$ || true)
        if [ "${HEADOBJ_ERROR_CODE}" -ne 0 ] || [ "${HEADOBJ_ERROR}" ] ;then
            HEADOBJ_HTTP_CODE=$(echo "${HEADOBJ_ERROR}" | sed 's/.*(\([0-9]*\)).*/\1/g' 2>/dev/null || echo "")
            HEADOBJ_REASON=$(echo "${HEADOBJ_ERROR}" | awk -F: '{print $NF}')
            echo "$(ts) : keepLockAlive ${KAL_PID} : checking lock file failed, unhandled error, will retry"
            echo "$(ts) : keepLockAlive ${KAL_PID} : error code: ${HEADOBJ_ERROR_CODE}"
            echo "$(ts) : keepLockAlive ${KAL_PID} : ${HEADOBJ_ERROR}"
            KEEPALIVE_RETRY=$((KEEPALIVE_RETRY + 1))
            if [ "${KEEPALIVE_RETRY}" -ge "${KEEPALIVE_MAX_RETRY}" ] ; then
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY} max reached, exiting"
                exit 63
            else
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY}"
            fi
        else
            KEEPALIVE_RETRY=0
        fi
        unset HEADOBJ_ERROR_CODE HEADOBJ_ERROR HEADOBJ_HTTP_CODE HEADOBJ_REASON

        PUTOBJ_ETAG=$(jq -r '.ETag // "error1"' ${TMP_DIR}/ka_putobj.$$.json | tr -d '"')
        HEADOBJ_ETAG=$(jq -r '.ETag // "error2"' ${TMP_DIR}/ka_headobj.$$.json | tr -d '"')

        if [ "${PUTOBJ_ETAG}" == "error1" ] || [ "${HEADOBJ_ETAG}" == "error2" ] || [ -z "${PUTOBJ_ETAG}" ] || [ -z "${HEADOBJ_ETAG}" ]; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : error extracting ETags"
            KEEPALIVE_RETRY=$((KEEPALIVE_RETRY + 1))
            if [ "${KEEPALIVE_RETRY}" -ge "${KEEPALIVE_MAX_RETRY}" ] ; then
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY} max reached, exiting"
                exit 64
            else
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY}"
            fi
            continue
        else
            KEEPALIVE_RETRY=0
        fi

        echo "$(ts) : keepLockAlive ${KAL_PID} : Etag on head metadata : ${HEADOBJ_ETAG} "

        if [ "${PUTOBJ_ETAG}" != "${HEADOBJ_ETAG}" ] ; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : ETAGs are different, possible lock intrusion 1: ${PUTOBJ_ETAG} 2: ${HEADOBJ_ETAG}"
            exit 65
        fi

        echo "$(ts) : keepLockAlive : $$ " > ${TMP_DIR}/ka_putobj_lock_file.$$
        aws s3api put-object --bucket "${BUCKET}" --key "${S3_LOCK_PATH}${LOCK_SUFFIX}" --if-match "${PUTOBJ_ETAG}" --metadata "{ \"ts\" : \"$(date -u +%s)\", \"pid\" : \"$$\", \"host\" : \"$(hostname)\", \"task\" : \"${TASK_NAME}\" }" --content-type "text/plain" --body ${TMP_DIR}/ka_putobj_lock_file.$$ > ${TMP_DIR}/ka_putobj.$$.json 2> ${TMP_DIR}/ka_putobj.$$.error
        PUTOBJ_ERROR_CODE=$?
        if [ ! -f "${TMP_DIR}/ka_putobj.$$.error" ]; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : error: expected error file not found"
            exit 66
        fi
        PUTOBJ_ERROR=$(cat ${TMP_DIR}/ka_putobj.$$.error | grep -v ^$ || true)
        if [ "${PUTOBJ_ERROR_CODE}" -ne 0 ] || [ "${PUTOBJ_ERROR}" ] ; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : error updating lock, error code ${PUTOBJ_ERROR_CODE}"
            echo "$(ts) : keepLockAlive ${KAL_PID} : ${PUTOBJ_ERROR}"
            cat ${TMP_DIR}/ka_putobj.$$.error
            echo "$(ts) : keepLockAlive ${KAL_PID} : retry in ${KEEPALIVE_LOOP_SLEEP_SEC}"
            sleep "${KEEPALIVE_LOOP_SLEEP_SEC}"
            KEEPALIVE_RETRY=$((KEEPALIVE_RETRY + 1))
            if [ "${KEEPALIVE_RETRY}" -ge "${KEEPALIVE_MAX_RETRY}" ] ; then
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY} max reached, exiting"
                exit 67
            else
                echo "$(ts) : keepLockAlive ${KAL_PID} : retry count ${KEEPALIVE_RETRY}/${KEEPALIVE_MAX_RETRY}"
            fi
            continue
        else
            KEEPALIVE_RETRY=0
        fi

        ETAG_OUTPUT=$(jq -r '.ETag // "error"' ${TMP_DIR}/ka_putobj.$$.json | tr -d '"')
        if [ "${ETAG_OUTPUT}" != "error" ] && [ -n "${ETAG_OUTPUT}" ]; then
            echo "$(ts) : keepLockAlive ${KAL_PID} : Etag after put : ${ETAG_OUTPUT}"
        else
            echo "$(ts) : keepLockAlive ${KAL_PID} : Etag after put : (error extracting)"
        fi
        unset PUTOBJ_ERROR_CODE PUTOBJ_ERROR
    done
}

########################
# main sequence
#

trap cleanup EXIT
trap 'echo "$(ts): received termination signal, cleaning up"; cleanup; exit 99' SIGINT SIGTERM

hash aws
hash jq
hash cat
hash echo
hash sleep
hash sed
hash hostname
hash seq
hash awk
hash grep
hash tr
hash date
hash mktemp
hash mkdir
hash basename
hash cp
hash kill
hash wait
hash bc

TMP_DIR=$(mktemp -d /tmp/lock_on_s3.XXXXXX)
mkdir -p ${TMP_DIR}


BNAME=$(basename "$0")

if [ $# -le 2 ]; then
    print_help
    exit 1
fi

# input args
CONF_FILE=${1:-"lock_on_s3.conf"}
PROTECTED_CMD=${2:-"sleep 120"}
TASK_NAME=${3:-"example_task_name"}
LOCK_SUFFIX=${4:-}


BASH_MAJOR="${BASH_VERSION%%.*}"
BASH_MINOR="${BASH_VERSION#*.}"
BASH_MINOR="${BASH_MINOR%%.*}"
if [ "${BASH_MAJOR}" -lt 5 ] || ([ "${BASH_MAJOR}" -eq 5 ] && [ "${BASH_MINOR}" -lt 1 ]); then
    echo "$(date -u --iso-8601=seconds) : error: bash 5.1+ required for wait -f -n -p (current: ${BASH_VERSION})"
    exit 2
fi

echo "utc-time : pid : task name : lock_suffix : process name : <message>"
echo "$(ts) : main : CONF_FILE : ->${CONF_FILE}<-"
echo "$(ts) : main : PROTECTED COMMAND : ->${PROTECTED_CMD}<-"
echo "$(ts) : main : TASK NAME : ->${TASK_NAME}<-"
echo "$(ts) : main : LOCK_SUFFIX : ->${LOCK_SUFFIX}<-"

if [ ! -f "${CONF_FILE}" ]; then
    echo "$(ts) : error: configuration file not found: ${CONF_FILE}"
    exit 3
fi

echo "$(ts) : main : reading from conf file :"
source "${CONF_FILE}"

if [ "${LOCK_STALE_SEC}" -lt "${KEEPALIVE_LOOP_SLEEP_SEC}" ]; then
    echo "$(ts) : error: LOCK_STALE_SEC (${LOCK_STALE_SEC}) must be >= KEEPALIVE_LOOP_SLEEP_SEC (${KEEPALIVE_LOOP_SLEEP_SEC})"
    exit 4
fi
if [ "${LOCK_STALE_SEC}" -lt 60 ]; then
    echo "$(ts) : warning: LOCK_STALE_SEC (${LOCK_STALE_SEC}) is very short, may cause false stale detections"
fi
if [ "${MAX_LOCK_DURATION_SEC}" -lt "${LOCK_STALE_SEC}" ]; then
    echo "$(ts) : error: MAX_LOCK_DURATION_SEC (${MAX_LOCK_DURATION_SEC}) should be >= LOCK_STALE_SEC (${LOCK_STALE_SEC})"
    exit 5
fi
if ! [[ "${GRACEFUL_SHUTDOWN_TIMEOUT_SEC}" =~ ^[0-9]+$ ]] || [ "${GRACEFUL_SHUTDOWN_TIMEOUT_SEC}" -lt 1 ]; then
    echo "$(ts) : error: GRACEFUL_SHUTDOWN_TIMEOUT_SEC must be a positive integer"
    exit 6
fi
if ! [[ "${MAX_RETRIES}" =~ ^[0-9]+$ ]] || [ "${MAX_RETRIES}" -lt 1 ]; then
    echo "$(ts) : error: MAX_RETRIES must be a positive integer"
    exit 7
fi
if ! [[ "${LOCK_LOOP_SLEEP_SEC}" =~ ^[0-9]+$ ]] || [ "${LOCK_LOOP_SLEEP_SEC}" -lt 1 ]; then
    echo "$(ts) : error: LOCK_LOOP_SLEEP_SEC must be a positive integer"
    exit 8
fi
if ! [[ "${LOCK_VERIFY_DELAY_SEC}" =~ ^[0-9]+$ ]] || [ "${LOCK_VERIFY_DELAY_SEC}" -lt 0 ]; then
    echo "$(ts) : error: LOCK_VERIFY_DELAY_SEC must be a non-negative integer"
    exit 9
fi
if ! [[ "${KEEPALIVE_LOOP_SLEEP_SEC}" =~ ^[0-9]+$ ]] || [ "${KEEPALIVE_LOOP_SLEEP_SEC}" -lt 1 ]; then
    echo "$(ts) : error: KEEPALIVE_LOOP_SLEEP_SEC must be a positive integer"
    exit 10
fi
if ! [[ "${MAX_LOCK_DURATION_SEC}" =~ ^[0-9]+$ ]] || [ "${MAX_LOCK_DURATION_SEC}" -lt 1 ]; then
    echo "$(ts) : error: MAX_LOCK_DURATION_SEC must be a positive integer"
    exit 11
fi
if ! [[ "${LOCK_STALE_SEC}" =~ ^[0-9]+$ ]] || [ "${LOCK_STALE_SEC}" -lt 1 ]; then
    echo "$(ts) : error: LOCK_STALE_SEC must be a positive integer"
    exit 12
fi
if ! [[ "${EXP_BACKOFF_COEF}" =~ ^[1-9][0-9]*.[0-9]*$ ]] ; then
    echo "$(ts) : error: EXP_BACKOFF_COEF must be correct "
    exit 13
fi
if ! [[ "${WAIT_STALE_TIME_SEC}" =~ ^[0-9]+$ ]] || [ "${WAIT_STALE_TIME_SEC}" -lt 0 ]; then
    echo "$(ts) : error: WAIT_STALE_TIME_SEC must be a non-negative integer"
    exit 14
fi
if [[ -z "${PROTECTED_CMD}" ]]; then
    echo "$(ts) : error: command must not be empty"
    exit 15
fi
if [[ "${PROTECTED_CMD}" =~ [\;\&\|\(\)\<\>\$\`\"] ]]; then
    echo "$(ts) : error: forbidden characters in PROTECTED_CMD"
    exit 16
fi



: "${BUCKET:?BUCKET must be set in conf file}"
: "${S3_LOCK_PATH:?S3_LOCK_PATH must be set in conf file}"
: "${S3_WAIT_PATH:?S3_WAIT_PATH must be set in conf file}"
: "${S3_LOCK_ACHIEVED_PATH:?S3_LOCK_ACHIEVED_PATH must be set in conf file}"
: "${MAX_LOCK_DURATION_SEC:?MAX_LOCK_DURATION_SEC must be set in conf file}"
: "${LOCK_STALE_SEC:?LOCK_STALE_SEC must be set in conf file}"
: "${LOCK_LOOP_SLEEP_SEC:?LOCK_LOOP_SLEEP_SEC must be set in conf file}"
: "${LOCK_VERIFY_DELAY_SEC:?LOCK_VERIFY_DELAY_SEC must be set in conf file}"
: "${KEEPALIVE_LOOP_SLEEP_SEC:?KEEPALIVE_LOOP_SLEEP_SEC must be set in conf file}"
: "${MAX_RETRIES:?MAX_RETRIES must be set in conf file}"
: "${GRACEFUL_SHUTDOWN_TIMEOUT_SEC:?GRACEFUL_SHUTDOWN_TIMEOUT_SEC must be set in conf file}"
: "${EXP_BACKOFF_COEF:?EXP_BACKOFF_COEF must be set in conf file}"
: "${WAIT_STALE_TIME_SEC:?WAIT_STALE_TIME_SEC must be set in conf file}"

echo "$(ts) : main : BUCKET : ->${BUCKET}<-"
echo "$(ts) : main : LOCK_PATH : ->${S3_LOCK_PATH}<-"
echo "$(ts) : main : S3_WAIT_PATH : ->${S3_WAIT_PATH}<-"
echo "$(ts) : main : S3_LOCK_ACHIEVED_PATH : ->${S3_LOCK_ACHIEVED_PATH}<-"
echo "$(ts) : main : MAX_LOCK_DURATION_SEC : ->${MAX_LOCK_DURATION_SEC}<-"
echo "$(ts) : main : LOCK_STALE_SEC : ->${LOCK_STALE_SEC}<-"
echo "$(ts) : main : LOCK_LOOP_SLEEP_SEC : ->${LOCK_LOOP_SLEEP_SEC}<-"
echo "$(ts) : main : LOCK_VERIFY_DELAY_SEC : ->${LOCK_VERIFY_DELAY_SEC}<-"
echo "$(ts) : main : KEEPALIVE_LOOP_SLEEP_SEC : ->${KEEPALIVE_LOOP_SLEEP_SEC}<-"
echo "$(ts) : main : MAX_RETRIES : ->${MAX_RETRIES}<-"
echo "$(ts) : main : GRACEFUL_SHUTDOWN_TIMEOUT_SEC : ->${GRACEFUL_SHUTDOWN_TIMEOUT_SEC}<-"
echo "$(ts) : main : EXP_BACKOFF_COEF : ->${EXP_BACKOFF_COEF}<-"

if [[ "${S3_LOCK_PATH}" =~ ^/ ]] || [[ "${S3_LOCK_PATH}" =~ /$ ]]; then
    echo "$(ts) : warning: S3_LOCK_PATH should not start or end with '/'"
fi
if [[ "${S3_WAIT_PATH}" =~ ^/ ]] || [[ "${S3_WAIT_PATH}" =~ /$ ]]; then
    echo "$(ts) : warning: S3_WAIT_PATH should not start or end with '/'"
fi
if [[ "${S3_LOCK_ACHIEVED_PATH}" =~ ^/ ]] || [[ "${S3_LOCK_ACHIEVED_PATH}" =~ /$ ]]; then
    echo "$(ts) : warning: S3_LOCK_ACHIEVED_PATH should not start or end with '/'"
fi
echo "$(ts) : main : --------------------------------------------"

MAIN_START_TIME=$(date -u --iso-8601=seconds)
MAIN_START_TIME_NANO=$(date -u +%s%N)

if [ "${PROTECTED_CMD}" != "unlock" ]; then
    echo "$(ts) : main : starting ${MAIN_START_TIME} ${MAIN_START_TIME_NANO}, pid $$, trying to get lock : "
    get_lock
    GETLOCK_RC=$?
    if [ "${GETLOCK_RC}" != "0" ] ; then
        echo "$(ts) : main : get_lock exited with ${GETLOCK_RC}" code, exiting"
        exit 17
    fi
else
    echo "$(ts) : main : starting, pid $$, trying to remove lock : "
    remove_lock
    exit 0
fi

ETAG_OUTPUT=$(jq -r '.ETag // "error"' ${TMP_DIR}/putobj.$$.json 2>/dev/null | tr -d '"' || echo "error")
if [ "${ETAG_OUTPUT}" != "error" ] && [ -n "${ETAG_OUTPUT}" ]; then
    echo "$(ts) : main : lock achieved, lock metadata ETAG : ${ETAG_OUTPUT} <-------- "
else
    echo "$(ts) : main : lock achieved, but could not extract ETAG <-------- "
fi


LOCK_HOLD_START=$(date -u +%s)

echo "$(ts) : main : spawning keep alive worker : "
keep_alive_lock &
KA_PID=$!
echo "$(ts) : main : keep alive worker pid : ${KA_PID}"


echo "$(ts) : main : spawning protected command : ${PROTECTED_CMD}"
# not doing eval or /bin/bash -c "${PROTECTED_CMD}" &
#OLD_IFS="${IFS}"
#IFS=' ' read -ra CMD_ARRAY <<< "${PROTECTED_CMD}"
#IFS="${OLD_IFS}"
#if [ ${#CMD_ARRAY[@]} -eq 0 ]; then
#    echo "$(ts) : main : error: PROTECTED_CMD is empty"
#    exit 16
#fi
#"${CMD_ARRAY[@]}" &

eval ${PROTECTED_CMD} &
PC_PID=$!
echo "$(ts) : main : protected command pid : ${PC_PID}"


echo "$(ts) : main : waiting for termination"
wait -f -n -p WPID ${KA_PID} ${PC_PID}
WAITRC=$?

if [ "${WPID}" -eq "${KA_PID}" ] ; then
    echo "$(ts) : main : pid ${WPID}, keep alive exited (exit code ${WAITRC}), terminating protected command"
    kill -15 ${PC_PID}

    for i in $(seq 1 ${GRACEFUL_SHUTDOWN_TIMEOUT_SEC}); do
        if ! kill -0 ${PC_PID} 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 ${PC_PID} 2>/dev/null; then
        echo "$(ts) : main : force killing ${PC_PID}"
        kill -9 ${PC_PID} 2>/dev/null
    fi
fi

if [ "${WPID}" -eq "${PC_PID}" ] ; then
    echo "$(ts) : main : pid ${WPID}, protected command returned, waitrc ${WAITRC}"
    echo "$(ts) : main : protected command (${PC_PID}) ${PROTECTED_CMD} exited, terminating keep alive worker ${KA_PID}"
    kill -15 ${KA_PID}

    for i in $(seq 1 ${GRACEFUL_SHUTDOWN_TIMEOUT_SEC}); do
        if ! kill -0 ${KA_PID} 2>/dev/null; then
            break  # Process already terminated
        fi
        sleep 1
    done
    if kill -0 ${KA_PID} 2>/dev/null; then
        echo "$(ts) : main : keep alive worker ${KA_PID} did not terminate, terminating forcefully"
        kill -9 ${KA_PID} 2> /dev/null
    fi
fi

delete_lock_file

LOCK_HOLD_END=$(date -u +%s)
LOCK_HOLD_DURATION=$((LOCK_HOLD_END - LOCK_HOLD_START))
echo "$(ts) : main : metrics : lock_hold_seconds=${LOCK_HOLD_DURATION}"

echo "$(ts) : main : exiting"
exit 0

