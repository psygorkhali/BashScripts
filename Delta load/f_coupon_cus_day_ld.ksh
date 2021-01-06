#!/bin/ksh

######################################################################
# Script : f_coupon_cus_day_ld.ksh
# Description : This script loads the Coupon Customer Day Base table
#               DWH_F_COUPON_CUS_DAY_B using SOURCE TABLE 
#				STG_F_COUPON_CUS_DAY_B.
# Modifications
# 10/02/2020  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
export DIM_CLOSABLE=0
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="STG_F_COUPON_CUS_DAY_B"
TARGET_TABLE="DWH_F_CUS_WALLET_B"
TEMP_TABLE="TMP_F_CUS_WALLET_B"

DIM_IDNT='COUPON_ID
CUS_ID
COUPON_REDMPTN_DT'
DIM_KEY=''

TEMP_TABLE_COLUMN='COUPON_ISSUE_DT
COUPON_DEF_ID
CUS_KEY
COUPON_KEY
TIMES_USED
COUPON_EXPR_DT
COUPON_CREATED_DT
COUPON_DISP_ORD
COUPON_UPDATED_DT
CERT_NUM
COUPON_STATUS
RING_CDE
CMPGN_KEY
INGEST_DT'

####### SET VARIABLES ######################################
set_variables
set_dimension_variable
print_msg "${SCRIPT_NAME} Started"
start_script

ARCHIVE_DIR="${DATA_DIR}/customer/data_archive/"
DATA_DIR="${DATA_DIR}/customer/couponcus/"
FILE_LIST=$(ls -1 ${DATA_DIR})
FILE_LIST=(${FILE_LIST[@]})
FILE_COUNT=${#FILE_LIST[@]}

if [[ ${FILE_COUNT} != 1  ]]
then
	print_msg "Unexpected number of data files encountered: ${FILE_COUNT}"
	print_msg "Expected 1 data file for processing at a time"
	#chk_err -r 1 -m "Expected 1 data file for processing at a time"
fi

DATA_FILE=${DATA_DIR}${FILE_LIST[0]}

##############################################
######### Loading DWH_F_CUS_WALLET_B #########
##############################################

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
	print_msg "Load into Temp Table Started"
	print_msg "Truncate Temp Table"
	
	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"

	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE} (
		${DIM_IDNT_LIST}
		,${TEMP_TABLE_COLUMN_LIST}
	)
	SELECT SRC.COUPON_ID
		,SRC.CUS_ID		
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_DEF_ID
		,COALESCE(CUS.CUS_KEY,-1) CUS_KEY
		,COALESCE(CPN.COUPON_KEY,-1) COUPON_KEY
		,SRC.TIMES_USED TIMES_USED
		,SRC.COUPON_EXPR_DT COUPON_EXPR_DT
		,SRC.COUPON_CREATED_DT COUPON_CREATED_DT
		,SRC.COUPON_DISP_ORD COUPON_DISP_ORD
		,SRC.COUPON_UPDATED_DT COUPON_UPDATED_DT
		,SRC.CERT_NUM CERT_NUM
		,SRC.COUPON_STATUS COUPON_STATUS
		,SRC.RING_CDE RING_CDE
		,SRC.CMPGN_KEY CMPGN_KEY
		,SRC.INGEST_DT INGEST_DT
	FROM ${SRC_DB}.${SOURCE_TABLE} SRC
	LEFT OUTER JOIN ${VIEW_DB}.V_DWH_D_CUS_LU CUS
		ON CUS.CUS_ID=SRC.CUS_ID
	LEFT OUTER JOIN ${VIEW_DB}.V_DWH_D_COUPON_LU CPN
		ON CPN.COUPON_NUM=SRC.RING_CDE
			AND CPN.CMPGN_KEY=SRC.CMPGN_KEY
			AND CPN.RCD_CLOSE_FLG=0
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_ID,SRC.COUPON_REDMPTN_DT
		ORDER BY SRC.INGEST_DT DESC)=1;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for Coupon History into temp table" 
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2
	set_bookmark "AFTER_LOAD_TEMP_TABLE"
fi

############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	update_using_temp_cdh
	set_bookmark "AFTER_UPDATE_FROM_TEMP"
fi

if [[ ${BOOKMARK} = "AFTER_UPDATE_FROM_TEMP" ]]
then
	insert_nokeydimension_from_temp
	set_bookmark "AFTER_INSERT_INTO_TEMP"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_INTO_TEMP" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(COUPON_ID,CUS_KEY)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE"
fi

##############################################
####### Loading DWH_F_COUPON_CUS_DAY_B #######
##############################################

SOURCE_TABLE="DWH_F_CUS_WALLET_B"
TARGET_TABLE="DWH_F_COUPON_CUS_DAY_B"
TEMP_TABLE="TMP_F_COUPON_CUS_DAY_B"

FACT_KEYS='COUPON_ID
CUS_KEY
DAY_KEY'

FACT_IDNT=''

OTHER_FIELDS='END_DAY_KEY
COUPON_DEF_ID
COUPON_KEY
COUPON_ISSUE_DT
COUPON_REDMPTN_DT
COUPON_EXPR_DT
COUPON_DISP_ORD
CERT_NUM
COUPON_STATUS
ISSUE_FLG
EXPR_FLG
REDEEM_FLG'

FACT_FIELDS='TIMES_USED'

####### SET VARIABLES ######################################
set_fact_variable

####### LOAD INTO TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE" ]]
then
	print_msg "Load into Temp Table Started"
	print_msg "Truncate Temp Table"
	
	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"

	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE} (
		${FACT_KEYS_LIST}		
		,${OTHER_FIELDS_LIST}
		,${FACT_FIELDS_LIST}
	)
	SELECT SRC.COUPON_ID
		,SRC.CUS_KEY
		,SRC.COUPON_ISSUE_DT DAY_KEY
		,SRC.COUPON_ISSUE_DT END_DAY_KEY
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_EXPR_DT
		,SRC.COUPON_DISP_ORD
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,1 ISSUE_FLG
		,CASE WHEN COUPON_ISSUE_DT=COUPON_EXPR_DT THEN 1 ELSE 0 END EXPR_FLG
		,CASE WHEN COUPON_ISSUE_DT=COUPON_REDMPTN_DT THEN 1 ELSE 0 END REDEEM_FLG
		,SRC.TIMES_USED
	FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B
		)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY ORDER BY SRC.INGEST_DT DESC)=1
	UNION ALL
	SELECT SRC.COUPON_ID
		,SRC.CUS_KEY
		,SRC.COUPON_REDMPTN_DT DAY_KEY
		,SRC.COUPON_REDMPTN_DT END_DAY_KEY
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_EXPR_DT
		,SRC.COUPON_DISP_ORD
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,0 ISSUE_FLG
		,CASE WHEN COUPON_REDMPTN_DT=COUPON_EXPR_DT THEN 1 ELSE 0 END EXPR_FLG
		,1 REDEEM_FLG
		,SRC.TIMES_USED
	FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B
		)
			AND COUPON_REDMPTN_DT IS NOT NULL
			AND COUPON_REDMPTN_DT>COUPON_ISSUE_DT
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY ORDER BY SRC.INGEST_DT DESC)=1
	UNION ALL
	SELECT SRC.COUPON_ID
		,SRC.CUS_KEY
		,SRC.COUPON_ISSUE_DT+1 DAY_KEY
		,SRC.COUPON_REDMPTN_DT-1 END_DAY_KEY
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_EXPR_DT
		,SRC.COUPON_DISP_ORD
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,0 ISSUE_FLG
		,0 EXPR_FLG
		,0 REDEEM_FLG
		,SRC.TIMES_USED
	FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B
		)
			AND SRC.COUPON_REDMPTN_DT IS NOT NULL
			AND SRC.COUPON_REDMPTN_DT>SRC.COUPON_ISSUE_DT+1
			AND SRC.COUPON_EXPR_DT>SRC.COUPON_ISSUE_DT+1
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY ORDER BY SRC.INGEST_DT DESC)=1
	UNION ALL
	SELECT SRC.COUPON_ID
		,SRC.CUS_KEY
		,SRC.COUPON_ISSUE_DT+1 DAY_KEY
		,SRC.COUPON_EXPR_DT-1 END_DAY_KEY
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_EXPR_DT
		,SRC.COUPON_DISP_ORD
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,0 ISSUE_FLG
		,0 EXPR_FLG
		,0 REDEEM_FLG
		,SRC.TIMES_USED
	FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B
		)
			AND SRC.COUPON_REDMPTN_DT IS NULL
			AND SRC.COUPON_EXPR_DT>SRC.COUPON_ISSUE_DT+1
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY ORDER BY SRC.INGEST_DT DESC)=1
	UNION ALL
	SELECT SRC.COUPON_ID
		,SRC.CUS_KEY
		,SRC.COUPON_EXPR_DT DAY_KEY
		,SRC.COUPON_EXPR_DT END_DAY_KEY
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.COUPON_ISSUE_DT
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_EXPR_DT
		,SRC.COUPON_DISP_ORD
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,0 ISSUE_FLG
		,1 EXPR_FLG
		,0 REDEEM_FLG
		,SRC.TIMES_USED
	FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_WALLET_B
		)
			AND SRC.COUPON_REDMPTN_DT IS NULL
			AND SRC.COUPON_EXPR_DT>SRC.COUPON_ISSUE_DT
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY ORDER BY SRC.INGEST_DT DESC)=1
	;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for Coupon Customer Day Base  into temp table" 
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${TARGET_DB} ${SOURCE_TABLE} "*" ${TEMP_TABLE}
	set_bookmark "AFTER_LOAD_TEMP_TABLE_1"
fi

####### COLLECT STATS FOR TEMP TABLE ##############
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE_1" ]]
then
	print_msg "Collecting Statistics for ${TEMP_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(COUPON_ID,CUS_KEY,DAY_KEY)
	ON ${TEMP_DB}.${TEMP_TABLE};
	"
	
	run_query -d "${TEMP_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Temp Table"
	print_msg "${TEMP_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TEMP_TABLE_1"
fi

############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TEMP_TABLE_1" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	print_msg "Deleting ${TARGET_TABLE}"		

	DELETE_SQL="
	DELETE FROM ${TARGET_DB}.${TARGET_TABLE} A
	WHERE EXISTS (
		SELECT 1 
		FROM ${TEMP_DB}.${TEMP_TABLE} B
		WHERE A.COUPON_ID=B.COUPON_ID
			AND A.CUS_KEY=B.CUS_KEY
		GROUP BY COUPON_ID
			, CUS_KEY
		HAVING A.DAY_KEY>=MIN(B.DAY_KEY)
	);
	"
	
	set_audit_log_var
	run_query -d "${TARGET_DB}" -q "${DELETE_SQL}" -m "Unable to Delete Coupon Customer Day Records from target table" 	
	print_msg "${TARGET_DB} deleted successfully"			
	set_activity_count delete
	audit_log 2 ${TARGET_DB} ${SOURCE_TABLE} "*" ${TARGET_TABLE}
	set_bookmark "AFTER_DELETE_FROM_TARGET_1"
fi

if [[ ${BOOKMARK} = "AFTER_DELETE_FROM_TARGET_1" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(COUPON_ID,CUS_KEY,DAY_KEY)
		, COLUMN(COUPON_ID)
		, COLUMN(DAY_KEY)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE_11"
fi

if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE_11" ]]
then
	insert_std_fact_from_temp_cdh
	set_bookmark "AFTER_INSERT_STD_FACT_FROM_TEMP_1"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_STD_FACT_FROM_TEMP_1" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(COUPON_ID,CUS_KEY,DAY_KEY)
		, COLUMN(COUPON_ID)
		, COLUMN(DAY_KEY)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE_1"
fi

##############################################
### Loading DWH_F_COUPON_REDMPTN_CUS_DAY_B ###
##############################################

SOURCE_TABLE="DWH_F_CUS_WALLET_B"
TARGET_TABLE="DWH_F_COUPON_REDMPTN_CUS_DAY_B"
TEMP_TABLE="TMP_F_COUPON_REDMPTN_CUS_DAY_B"

FACT_KEYS='COUPON_ID
CUS_KEY
COUPON_REDMPTN_DT'

FACT_IDNT=''

OTHER_FIELDS='COUPON_DEF_ID
COUPON_KEY
CERT_NUM
COUPON_STATUS'

FACT_FIELDS='TIMES_USED'

####### SET VARIABLES ######################################
set_fact_variable

####### LOAD INTO TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE_1" ]]
then
	print_msg "Load into Temp Table Started"
	print_msg "Truncate Temp Table"
	
	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"

	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE} (
		${FACT_KEYS_LIST}		
		,${OTHER_FIELDS_LIST}
		,${FACT_FIELDS_LIST}
	)
	SELECT SRC.COUPON_ID		
		,SRC.CUS_KEY
		,SRC.COUPON_REDMPTN_DT
		,SRC.COUPON_DEF_ID
		,SRC.COUPON_KEY		
		,SRC.CERT_NUM
		,SRC.COUPON_STATUS
		,SRC.TIMES_USED
	FROM ${TARGET_DB}.${SOURCE_TABLE} SRC
	WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${TARGET_DB}.${SOURCE_TABLE}
		)
		AND SRC.COUPON_REDMPTN_DT IS NOT NULL
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC.COUPON_ID,SRC.CUS_KEY,SRC.COUPON_REDMPTN_DT ORDER BY SRC.INGEST_DT DESC)=1;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for Reward Customer Day Base  into temp table" 
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${TARGET_DB} ${SOURCE_TABLE} "*" ${TEMP_TABLE}
	set_bookmark "AFTER_LOAD_TEMP_TABLE_2"
fi

############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE_2" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	update_std_fact_from_temp
	set_bookmark "AFTER_UPDATE_FROM_TEMP_2"
fi

if [[ ${BOOKMARK} = "AFTER_UPDATE_FROM_TEMP_2" ]]
then
	insert_std_fact_from_temp_cdh
	set_bookmark "AFTER_INSERT_STD_FACT_FROM_TEMP_2"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_STD_FACT_FROM_TEMP_2" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(COUPON_ID,CUS_KEY,COUPON_REDMPTN_DT)
		, COLUMN(COUPON_REDMPTN_DT)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE_2"
fi

############### DATA FILE POST-PROCESS ########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE_2" ]]
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
