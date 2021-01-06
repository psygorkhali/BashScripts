#!/bin/ksh

######################################################################
# Script : f_points_cus_yr_ld.ksh
# Description : This script loads the Points data
#               table DWH_F_POINTS_CUS_YR_A using SOURCE TABLE 
#				DWH_F_POINTS_CUS_DAY_B.
# Modifications
# 12/03/2020  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="DWH_F_POINTS_CUS_DAY_B"
TARGET_TABLE="DWH_F_POINTS_CUS_YR_A"
TEMP_TABLE="TMP_F_POINTS_CUS_YR_A"

FACT_KEYS='CUS_KEY
YR_KEY'

OTHER_FIELDS='CUS_HIST_KEY'

FACT_FIELDS='EARNED_POINTS
CUM_EARNED_POINTS
CONSUMED_POINTS
CUM_CONSUMED_POINTS
EXPIRED_POINTS
CUM_EXPIRED_POINTS
AVAILABLE_POINTS'

####### SET VARIABLES ######################################
set_variables
print_msg "${SCRIPT_NAME} Started"
start_script

####### SET CURRENT YEAR KEY ######################################
print_msg "Getting CURR_YR_KEY from DWH_D_CURR_TIM_LU"

GET_CURR_YR_KEY_SQL="
SELECT TRIM(CURR_YR_KEY)
FROM ${VIEW_DB}.V_DWH_D_CURR_TIM_LU;
"

CURR_YR_KEY=$(get_result -d "${VIEW_DB}" -q "$GET_CURR_YR_KEY_SQL" -m "Unable to get Current year Key")
print_msg "The current year key is : ${CURR_YR_KEY}"
export CURR_YR_KEY

####### LOAD INTO TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "NONE" ]]
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
	SELECT 
		SRC.CUS_KEY
		,YR_KEY
		,-1
		,SRC.EARNED_POINTS
		,SRC.CUM_EARNED_POINTS
		,SRC.CONSUMED_POINTS
		,SRC.CUM_CONSUMED_POINTS
		,SRC.EXPIRED_POINTS
		,SRC.CUM_EXPIRED_POINTS
		,SRC.AVAILABLE_POINTS
	FROM (
		SELECT *
		FROM ${VIEW_DB}.V_DWH_F_CUS_POINTS_B SRC
		WHERE RCD_UPD_TS=(
			SELECT MAX(RCD_UPD_TS) 
			FROM ${VIEW_DB}.V_DWH_F_CUS_POINTS_B
		)
	) 
	SRC
	INNER JOIN ${VIEW_DB}.V_DWH_D_TIM_DAY_LU TIM
		ON TIM.DAY_KEY BETWEEN SRC.EFF_FROM_DT AND SRC.EFF_TO_DT
			AND TIM.DAY_KEY<=CURRENT_DATE
	QUALIFY TIM.DAY_KEY=MAX(TIM.DAY_KEY)
		OVER (PARTITION BY CUS_KEY,YR_KEY);
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for points into temp table"
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${TARGET_DB} ${SOURCE_TABLE} "*" ${TEMP_TABLE}
	
	####### SET MIN WK KEY ######################################
	print_msg "Getting MIN_YR_KEY from ${TEMP_TABLE}"
	
	GET_MIN_YR_KEY_SQL="
	SELECT TRIM(MIN(YR_KEY))
	FROM ${TEMP_DB}.${TEMP_TABLE};
	"
	
	MIN_YR_KEY=$(get_result -d "${VIEW_DB}" -q "$GET_MIN_YR_KEY_SQL" -m "Unable to get Minimum year Key")
	print_msg "The minimum half year key is : ${MIN_YR_KEY}"
	export MIN_YR_KEY	
	set_bookmark "AFTER_LOAD_TEMP_TABLE"
fi

####### COLLECT STATS FOR TEMP TABLE ##############
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE" ]]
then
	print_msg "Collecting Statistics for ${TEMP_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,YR_KEY)
		,COLUMN(CUS_KEY)
		,COLUMN(YR_KEY)
	ON ${TEMP_DB}.${TEMP_TABLE};
	"

	run_query -d "${TEMP_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Temp Table"
	print_msg "${TEMP_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TEMP_TABLE"
fi

####### UPDATE TEMP TABLE ###############################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TEMP_TABLE" ]]
then		
	print_msg "Update Temp Table Started"
		
	UPDATE_SQL="
	UPDATE TMP
	FROM ${TEMP_DB}.${TEMP_TABLE} TMP
		, (
			SELECT CUS_KEY
				, YR_KEY
				, CUS_HIST_KEY
			FROM ${VIEW_DB}.DV_DWH_D_CUS_AS_OF_YR_LU
			WHERE YR_KEY BETWEEN ${MIN_YR_KEY}
				AND ${CURR_YR_KEY} 
		)LU
	SET CUS_HIST_KEY=LU.CUS_HIST_KEY
	WHERE TMP.CUS_KEY=LU.CUS_KEY
		AND TMP.YR_KEY=LU.YR_KEY;
	"

	set_audit_log_var
	run_query -d "${TEMP_DB}" -q "${UPDATE_SQL}" -m "Unable to Update Points year Records in temp table" 	
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count update
	print_msg "NO OF ROWS UPDATED: `if [[ $NO_OF_ROW_UPDATED -eq 0 ]]; then echo "0"; else echo "${NO_OF_ROW_UPDATED}"; fi`"
	set_bookmark "AFTER_UPDATE_TEMP_TABLE_1"
fi

############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_UPDATE_TEMP_TABLE_1" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	print_msg "Dropping Secondary Index for ${TARGET_TABLE} if exists"
		
	DROP_SQL="
	SELECT 1 FROM DBC.INDICES WHERE DATABASENAME='${TARGET_DB}' AND TABLENAME='${TARGET_TABLE}' AND COLUMNNAME='CUS_HIST_KEY';
	.if activitycount = 0 then GoTo ok
	DROP INDEX (CUS_HIST_KEY) ON ${TARGET_DB}.${TARGET_TABLE};
	.label ok
	"
	
	run_query -d "${TARGET_DB}" -q "${DROP_SQL}" -m "Unable to drop secondary index for Target Table"
	print_msg "${TARGET_TABLE} Secondary Index dropped successfully if exists"
	print_msg "Deleting ${TARGET_TABLE}"
	
	DELETE_SQL="
	DELETE FROM ${TARGET_DB}.${TARGET_TABLE} A
	WHERE EXISTS (
		SELECT 1
		FROM ${TEMP_DB}.${TEMP_TABLE} B
		WHERE A.CUS_KEY=B.CUS_KEY
			AND A.YR_KEY>=B.YR_KEY
	);
	"

	set_audit_log_var
	run_query -d "${TARGET_DB}" -q "${DELETE_SQL}" -m "Unable to Delete points records from target table"
	print_msg "${TARGET_DB} deleted successfully"
	set_activity_count delete
	audit_log 3
	set_bookmark "AFTER_DELETE_FROM_TARGET"
fi

if [[ ${BOOKMARK} = "AFTER_DELETE_FROM_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,YR_KEY)
		, COLUMN(CUS_KEY)
		, COLUMN(YR_KEY)
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
		 ${FACT_KEYS_LIST}
		,${OTHER_FIELDS_LIST}
		,${FACT_FIELDS_LIST}
		,RCD_INS_TS
		,RCD_UPD_TS
	)
	SELECT CUS_KEY
		,YR_KEY
		,CUS_HIST_KEY
		,EARNED_POINTS
		,CUM_EARNED_POINTS
		,CONSUMED_POINTS
		,CUM_CONSUMED_POINTS
		,EXPIRED_POINTS
		,CUM_EXPIRED_POINTS
		,AVAILABLE_POINTS
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
	FROM ${TEMP_DB}.${TEMP_TABLE} SRC
		WHERE NOT EXISTS (
			SELECT 1
			FROM ${VIEW_DB}.V_DWH_F_POINTS_CUS_YR_A TGT
			WHERE SRC.CUS_KEY=TGT.CUS_KEY
				AND SRC.YR_KEY=TGT.YR_KEY
		);
	"

	run_query -d "${TARGET_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Points Records into target table"
	print_msg "${TARGET_DB} loaded successfully"
	set_activity_count insert
	audit_log 3
	
	print_msg "Creating Secondary Index for ${TARGET_TABLE}"
		
	CREATE_SQL="
	CREATE INDEX (CUS_HIST_KEY) ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${CREATE_SQL}" -m "Unable to create secondary index for Target Table"
	print_msg "${TARGET_TABLE} Secondary Index created successfully"	
	set_bookmark "AFTER_INSERT_INTO_TARGET"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_INTO_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,YR_KEY)
		,COLUMN(CUS_KEY)
		,COLUMN(YR_KEY)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"

	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "DONE"
fi

script_successful
