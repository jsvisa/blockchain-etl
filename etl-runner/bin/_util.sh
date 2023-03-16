#!/bin/bash

function info() {
    echo "-----------------------------------"
    echo "[$(date)] $1"
    echo "-----------------------------------"
}

# function failure() {
#     local lineno=$2
#     local fn=$3
#     local exitstatus=$4
#     local msg=$5
#     local lineno_fns=${1% 0}
#     if [[ "$lineno_fns" != "0" ]] ; then
#         lineno="${lineno} ${lineno_fns}"
#     fi
#     echo "${BASH_SOURCE[1]}:${fn}[${lineno}] Failed with status ${exitstatus}: $msg"
# }
# trap 'failure "${BASH_LINENO[*]}" "$LINENO" "${FUNCNAME[*]:-script}" "$?" "$BASH_COMMAND"' ERR
