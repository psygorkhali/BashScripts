#!/bin/ksh
######################################################################
# Script : d_cus_tier_ld.ksh
# Description : This script loads the Customer Tier 
#               table DWH_D_CUS_TIER_LU using SOURCE TABLE 
#				STG_D_CUS_TIER_LU.
# Modifications
# 09/16/2019  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
export DIM_CLOSABLE=1
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="STG_D_CUS_TIER_LU"
TARGET_TABLE="DWH_D_CUS_TIER_LU"
TEMP_TABLE="TMP_${TARGET_TABLE#DWH_}"

DIM_IDNT='TIER_ID'
DIM_KEY='TIER_KEY'

TEMP_TABLE_COLUMN='TIER_SRC
   TIER_NAME
   TIER_DESC
   MIN_POINTS_FOR_TIER
   MAX_POINTS_FOR_TIER
   TIER_POINT_TYPE_NAME
   EXPR_PERIOD
   EXPR_PERIOD_UNIT
   ACTIVITY_PERIOD
   ACTIVITY_PERIOD_METHOD
   ACTIVITY_PERIOD_UNIT
   MOBILE_IMAGE_INDEX
   EXPIRE_DATE_EXPR 
   ACTIVITY_PERIOD_START_EXPR 
   ACTIVITY_PERIOD_END_EXPR
   POINT_EVENT_NAMES
   ADD_TO_ENRLMNT_DATE
   DISPLAY_TEXT'

####### SET VARIABLES ######################################
set_variables
print_msg "${SCRIPT_NAME} Started"
start_script

ARCHIVE_DIR="${DATA_DIR}/customer/data_archive/"   
DATA_DIR="${DATA_DIR}/customer/tier/"     
PREFIX_FILE_NAME="`(echo "${SOURCE_TABLE#STG_}" | tr [:upper:] [:lower:])`"
FILE_LIST=$(ls -1 ${DATA_DIR}| grep ${PREFIX_FILE_NAME})
FILE_LIST=(${FILE_LIST[@]})
FILE_COUNT=${#FILE_LIST[@]}

if [[ ${FILE_COUNT} != 1  ]]
then
	print_msg "Unexpected number of data files encountered: ${FILE_COUNT}"
	print_msg "Expected 1 data file for processing at a time"
	chk_err -r 1 -m "Expected 1 data file for processing at a time"
fi

DATA_FILE=${DATA_DIR}${FILE_LIST[0]}


####### LOAD INTO STAGING TABLE ##############
if [[ ${BOOKMARK} = "NONE" ]]
then
    truncate_staging_table	
    load_data
	set_bookmark "AFTER_STG_LOAD"
fi

####### LOAD INTO TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_STG_LOAD" ]]
then
	print_msg "Load into Temporary Table Started"
	print_msg "Truncate Temporary Table"
	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"
	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"
	
	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE}( 
		${DIM_IDNT}
		,${TEMP_TABLE_COLUMN_LIST}
		,RUN_DT
	)
	SELECT TIER_ID,
	TIER_SRC,
   TIER_NAME,
   TIER_DESC,
   MIN_POINTS_FOR_TIER,
   MAX_POINTS_FOR_TIER,
   TIER_POINT_TYPE_NAME,
   EXPR_PERIOD,
   EXPR_PERIOD_UNIT,
   ACTIVITY_PERIOD,
   ACTIVITY_PERIOD_METHOD,
   ACTIVITY_PERIOD_UNIT,
   MOBILE_IMAGE_INDEX,
   EXPIRE_DATE_EXPR, 
   ACTIVITY_PERIOD_START_EXPR, 
   ACTIVITY_PERIOD_END_EXPR,
   POINT_EVENT_NAMES,
   ADD_TO_ENRLMNT_DATE,
   DISPLAY_TEXT,
   RUN_DT
		FROM ${SRC_DB}.${SOURCE_TABLE} SRC;
	"
	
	run_query -d "${TEMP_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Records for Customer Tier records into temp table" 
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2
	set_bookmark "AFTER_LOAD_TEMP_TABLE"
fi
	
############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	update_using_temp
    set_bookmark "AFTER_UPDATE_USING_TEMP"
fi

if [[ ${BOOKMARK} = "AFTER_UPDATE_USING_TEMP" ]]
then
	insert_from_temp
	set_bookmark "AFTER_LOAD_TARGET_TABLE"
	print_msg "${TARGET_TABLE} Load Complete"
fi

############### DATA FILE POST-PROCESS ########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TARGET_TABLE" ]]
then
	if [[ -f ${DATA_FILE} ]]
	then
		print_msg "Adding checksum to data file for archiving"
		add_file_checksum
		print_msg "Archiving data file from ${DATA_FILE} to ${ARCHIVE_DIR}"
		mv -f ${DATA_FILE} ${ARCHIVE_DIR}
		set_bookmark "DONE"
	fi
fi

script_successful
