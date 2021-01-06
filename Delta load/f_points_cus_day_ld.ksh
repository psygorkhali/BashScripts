#!/bin/ksh

######################################################################
# Script : f_points_cus_day_ld.ksh
# Description : This script loads the Points Fact Data table 
#               DWH_F_POINTS_CUS_DAY_B using TEMP TABLE 
#				STG_F_POINTS_CUS_DAY_B.
# Modifications
# 12/7/2020  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
export DIM_CLOSABLE=0
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="STG_F_POINTS_CUS_DAY_B"
TEMP0_TABLE="TMP0_F_POINTS_CUS_DAY_B"
TEMP_TABLE="TMP_F_POINTS_CUS_DAY_B"
TARGET_TABLE="DWH_F_POINTS_CUS_DAY_B"

FACT_KEYS='CUS_KEY 
DAY_KEY
NEXT_DAY_KEY'

OTHER_FIELDS='
LYLTY_PROG_YR_KEY
EARNED_POINTS
CUM_EARNED_POINTS
CUM_PROG_YR_EARNED_POINTS
CONSUMED_POINTS
CUM_CONSUMED_POINTS
EXPIRED_POINTS
CUM_EXPIRED_POINTS
'

FACT_FIELDS='AVAILABLE_POINTS'

###### SET VARIABLES ######################################
set_variables
set_dimension_variable
print_msg "${SCRIPT_NAME} Started"
start_script

ARCHIVE_DIR="${DATA_DIR}/customer/data_archive/"
DATA_DIR="${DATA_DIR}/customer/points/"
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

####### LOAD INTO STAGING TABLE ##############
if [[ ${BOOKMARK} = "NONE" ]]
then

    truncate_staging_table	
    load_data
	print_msg "${SOURCE_TABLE} Stage load completed successfully"	
	set_bookmark "AFTER_STG_LOAD"
	
fi

####### LOAD INTO FIRST TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_STG_LOAD" ]]
then
	print_msg "Load into Temp0 Table Started"
	print_msg "Truncate Temp0 Table"

	truncate_table -d "${TEMP_DB}" -t "${TEMP0_TABLE}"

	print_msg "${TEMP0_TABLE} truncated successfully"
	print_msg "Loading ${TEMP0_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP0_TABLE} (
		${FACT_KEYS_LIST}
		,${OTHER_FIELDS_LIST}
		,${FACT_FIELDS_LIST}
		,CUS_ID
		,RUN_DT
	)
	SELECT CUS.CUS_KEY, 
	SRC.DAY_KEY,
	COALESCE(
			MIN(SRC.DAY_KEY) OVER (
				PARTITION BY CUS_KEY
				ORDER BY SRC.DAY_KEY ROWS BETWEEN 1 FOLLOWING 
					AND 1 FOLLOWING
			), 
			CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD')
		) AS NEXT_DAY_KEY,
	SRC.LYLTY_PROG_YR_KEY,
	SRC.EARNED_POINTS,
	SRC.CUM_EARNED_POINTS,
	SRC.CUM_PROG_YR_EARNED_POINTS,
	SRC.CONSUMED_POINTS,
	SRC.CUM_CONSUMED_POINTS,
	SRC.EXPIRED_POINTS,
	SRC.CUM_EXPIRED_POINTS,
	SRC.AVAILABLE_POINTS,
	SRC.CUS_ID,
	SRC.RUN_DT
	FROM ${SRC_DB}.${SOURCE_TABLE} SRC
	INNER JOIN ${VIEW_DB}.V_DWH_D_CUS_LU CUS
	ON SRC.CUS_ID=CUS.CUS_ID;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for points into first temp table"
	print_msg "${TEMP0_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${SRC_DB} ${SOURCE_TABLE} "*" ${TEMP0_TABLE}
	set_bookmark "AFTER_LOAD_FIRST_TEMP_TABLE"
fi

####### COLLECT STATS FOR FIRST TEMP TABLE ##############
if [[ ${BOOKMARK} = "AFTER_LOAD_FIRST_TEMP_TABLE" ]]
then
	print_msg "Collecting Statistics for ${TEMP_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,DAY_KEY)
		,COLUMN(CUS_KEY)
		,COLUMN(DAY_KEY)
	ON ${TEMP_DB}.${TEMP0_TABLE};
	"

	run_query -d "${TEMP_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for first Temp Table"
	print_msg "${TEMP0_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_FIRST_TEMP_TABLE"
fi

####### LOAD INTO SECOND TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_FIRST_TEMP_TABLE" ]]
then
	print_msg "Load into Temp Table Started"
	print_msg "Truncate Temp Table"

	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"

	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE} (
		 CUS_KEY
		,EFF_FROM_DT
		,EFF_TO_DT
		,${OTHER_FIELDS_LIST}		
		,${FACT_FIELDS_LIST}
	)
	SELECT 
		 CUS_KEY
		,BEGIN(A.PD) EFF_FROM_DT
		,END(A.PD) EFF_TO_DT
		,${OTHER_FIELDS_LIST}
		,${FACT_FIELDS_LIST}
	FROM (
		SELECT NORMALIZE 
		CUS_KEY, 
		LYLTY_PROG_YR_KEY,
		EARNED_POINTS,
		CUM_EARNED_POINTS,
		CUM_PROG_YR_EARNED_POINTS,
		CONSUMED_POINTS,
		CUM_CONSUMED_POINTS,
		EXPIRED_POINTS,
		CUM_EXPIRED_POINTS,
		AVAILABLE_POINTS,  
		PERIOD(DAY_KEY, NEXT_DAY_KEY) PD
		FROM ${TEMP_DB}.${TEMP0_TABLE}
		) A;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for points into second temp table"
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${TTEMP_DB} ${TEMP0_TABLE} "*" ${TEMP_TABLE}
	set_bookmark "AFTER_LOAD_SECOND_TEMP_TABLE"
fi

####### COLLECT STATS FOR TEMP TABLE ##############
if [[ ${BOOKMARK} = "AFTER_LOAD_SECOND_TEMP_TABLE" ]]
then
	print_msg "Collecting Statistics for ${TEMP_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,EFF_FROM_DT,EFF_TO_DT)
		,COLUMN(CUS_KEY)
		,COLUMN(EFF_FROM_DT)
		,COLUMN(EFF_TO_DT)
	ON ${TEMP_DB}.${TEMP_TABLE};
	"

	run_query -d "${TEMP_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Temp Table"
	print_msg "${TEMP_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_SECOND_TEMP_TABLE"
fi



############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_SECOND_TEMP_TABLE" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	print_msg "Deleting record from ${TARGET_TABLE} table whose effective-from-date is >= min of ${TEMP_TABLE} table effective-from-date"

	DELETE_SQL="
	DELETE FROM EXPDW_ETE_DWH.DWH_F_POINTS_CUS_DAY_B A
	WHERE EXISTS (
		SELECT 1
		FROM EXPDW_ETE_TMP.TMP_F_POINTS_CUS_DAY_B B
		WHERE A.CUS_KEY=B.CUS_KEY GROUP BY CUS_KEY
			HAVING A.EFF_FROM_DT>=min(B.EFF_FROM_DT)
			);
	"

	set_audit_log_var
	run_query -d "${TARGET_DB}" -q "${DELETE_SQL}" -m "Unable to Delete Records from target table"
	print_msg "${TARGET_DB} deleted successfully"
	set_activity_count delete
	audit_log 3
	set_bookmark "AFTER_DELETE_FROM_TARGET"
fi

if [[ ${BOOKMARK} = "AFTER_DELETE_FROM_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,EFF_FROM_DT)
		, COLUMN(CUS_KEY)
		, COLUMN(EFF_FROM_DT)
		, COLUMN(EFF_TO_DT)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"

	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE"
fi

if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE" ]]
then
	print_msg "Inserting ${TARGET_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TARGET_DB}.${TARGET_TABLE} (
		 CUS_KEY
		,EFF_FROM_DT
		,EFF_TO_DT
		,LYLTY_PROG_YR_KEY
		,EARNED_POINTS
		,CUM_EARNED_POINTS
		,CUM_LYLTY_PROG_YR_EARNED_POINTS
		,CONSUMED_POINTS
		,CUM_CONSUMED_POINTS
		,EXPIRED_POINTS
		,CUM_EXPIRED_POINTS
		,AVAILABLE_POINTS
		,RCD_INS_TS
		,RCD_UPD_TS
	)
	SELECT 
		 CUS_KEY
		,EFF_FROM_DT
		,EFF_TO_DT
		,LYLTY_PROG_YR_KEY
		,EARNED_POINTS
		,CUM_EARNED_POINTS
		,CUM_PROG_YR_EARNED_POINTS
		,CONSUMED_POINTS
		,CUM_CONSUMED_POINTS
		,EXPIRED_POINTS
		,CUM_EXPIRED_POINTS
		,AVAILABLE_POINTS
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
	FROM ${TEMP_DB}.${TEMP_TABLE};
	"

	run_query -d "${TARGET_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Records into target table"
	print_msg "${TARGET_DB} loaded successfully"
	set_activity_count insert
	audit_log 3
	set_bookmark "AFTER_INSERT_INTO_TARGET"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_INTO_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,EFF_FROM_DT)
		,COLUMN(CUS_KEY)
		,COLUMN(EFF_FROM_DT)
		,COLUMN(EFF_TO_DT)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"

	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER COLLECT STATS FOR TARGET TABLE"
fi


############### DATA FILE POST-PROCESS ########################
if [[ ${BOOKMARK} = "AFTER COLLECT STATS FOR TARGET TABLE" ]]
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
