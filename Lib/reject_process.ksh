#!/bin/ksh

#######################################################################
# Script :		reject_process.ksh                                    #
# Description : This script takes Reject table name as an argument to #
#               checks if the reject table data has been processed and#
#				updates the record in REJECT_PROCESS_TABLE            #
# Modifications                                                       #
# 09/23/2014  : Logic  : Initial Script                              #
#######################################################################

export SCRIPT_NAME="$(echo "${1%_*}" | tr [:upper:] [:lower:])"

. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

REJ_TABLE=$1
REJECT_PROCESS_TABLE="DWH_C_REJ_TBL_PROCESS"

function reject_processing
{
		print_msg "Checking if Reject table exists"
		check_rej_rec_sql="SELECT COUNT(*) FROM ${VIEW_DB}.${VIEW_PREFIX}${REJECT_PROCESS_TABLE} WHERE REJECT_TABLE_NAME='${REJ_TABLE}' AND PROCESS_READY_STATUS='N'"
		rej_rec_exist=$(get_result -d "${VIEW_DB}" -q "$check_rej_rec_sql" -m "Unable to run query")
		
		if [[ rej_rec_exist -ge 1 ]]
		then
			REJ_UPD_SQL="UPDATE ${TARGET_DB}.${REJECT_PROCESS_TABLE}
			SET  PROCESS_READY_STATUS='Y'
			WHERE REJECT_TABLE_NAME='${REJ_TABLE}'
			AND PROCESS_READY_STATUS='N' "
			run_query -d "${TARGET_DB}" -q "${REJ_UPD_SQL}" -m "Unable to Update Records into rejection table" 
		elif [[ rej_rec_exist -eq 0 ]]
		then
			echo "${REJ_TABLE} table record does not exist in Reject process table"
			exit 1;
		else 
			echo "Invalid number of record in ${REJECT_PROCESS_TABLE} for table ${REJ_TABLE}"
			exit 1;
		fi
		
	

}
##############################

if [[ $# == 1 ]]
then
	reject_processing
	print_msg "Reject data load ready"
	exit 1
else 
	echo "Usage: reject_process.ksh <REJECT_TABLENAME>"
	exit 1
fi