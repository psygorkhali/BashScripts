#!/bin/ksh

######################################################################
# Script : f_points_cus_qtr_history_ld.ksh
# Description : This script loads the Points data
#               table DWH_F_POINTS_CUS_QTR_A using SOURCE TABLE 
#				DWH_F_POINTS_CUS_DAY_B.
# Modifications
# 12/03/2020  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="DWH_F_POINTS_CUS_DAY_B"
TARGET_TABLE="DWH_F_POINTS_CUS_QTR_A"
TEMP_TABLE="TMP_F_POINTS_CUS_QTR_A"

FACT_KEYS='CUS_KEY
QTR_KEY'

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

####### SET QTR KEY ######################################
print_msg "Getting QTR_KEY from DWH_D_TIM_DAY_LU"

GET_HALFYR_SEG_SQL="
SELECT 'BETWEEN.'''||TRIM(HALFYR_START_DT)||'''.AND.'''||TRIM(HALFYR_END_DT)||''''
||'|'||'BETWEEN.'''||TRIM(MIN_QTR_KEY)||'''.AND.'''||TRIM(MAX_QTR_KEY)||''''
FROM (
	SELECT  HALFYR_START_DT
		,CASE WHEN CURRENT_DATE<HALFYR_END_DT THEN CURRENT_DATE ELSE HALFYR_END_DT END HALFYR_END_DT
		,MIN(QTR_KEY) MIN_QTR_KEY
		,MAX(QTR_KEY) MAX_QTR_KEY
	FROM EXPDW_ETE_DWH_V.V_DWH_D_TIM_DAY_LU
	WHERE YR_KEY IN (2018,2019,2020)
	GROUP BY 1,2
) A
ORDER BY 1;
"

GET_HALFYR_SEG=$(get_result -d "${VIEW_DB}" -q "$GET_HALFYR_SEG_SQL" -m "Unable to get HALFYR Seg")
print_msg "The halfyr segments are ${GET_HALFYR_SEG}"
export GET_HALFYR_SEG

for HALFYR_SEG in $GET_HALFYR_SEG
do

	IFS="|"
	set -A ARR_HALF_YR_SEG ${HALFYR_SEG}	  
	HALFYR_CLAUSE=$(echo ${ARR_HALF_YR_SEG[0]}|tr -t "." " ")
	QTR_CLAUSE=$(echo ${ARR_HALF_YR_SEG[1]} | tr "." " ")
	
	print_msg "HALF YEAR CLAUSE:=${HALFYR_CLAUSE}"
	print_msg "QTR KEY CLAUSE:=${QTR_CLAUSE}"
	
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
			,QTR_KEY
			,-1
			,SRC.EARNED_POINTS
			,SRC.CUM_EARNED_POINTS
			,SRC.CONSUMED_POINTS
			,SRC.CUM_CONSUMED_POINTS
			,SRC.EXPIRED_POINTS
			,SRC.CUM_EXPIRED_POINTS
			,SRC.AVAILABLE_POINTS
		FROM ${VIEW_DB}.DV_DWH_F_CUS_POINTS_B SRC
		INNER JOIN ${VIEW_DB}.V_DWH_D_TIM_DAY_LU TIM
			ON TIM.DAY_KEY=SRC.DAY_KEY
				AND SRC.DAY_KEY ${HALFYR_CLAUSE}
		QUALIFY TIM.DAY_KEY=MAX(TIM.DAY_KEY)
			OVER (PARTITION BY CUS_KEY,QTR_KEY);
		"
		
		run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for points into temp table"
		print_msg "${TEMP_TABLE} loaded successfully"
		set_activity_count insert
		audit_log 2 ${TARGET_DB} ${SOURCE_TABLE} "*" ${TEMP_TABLE}		
		set_bookmark "AFTER_LOAD_TEMP_TABLE"
	fi
	
	####### COLLECT STATS FOR TEMP TABLE ##############
	if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE" ]]
	then
		print_msg "Collecting Statistics for ${TEMP_TABLE}"
	
		STATS_SQL="
		COLLECT STATS COLUMN(CUS_KEY,QTR_KEY)
			,COLUMN(CUS_KEY)
			,COLUMN(QTR_KEY)
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
					, QTR_KEY
					, CUS_HIST_KEY
				FROM ${VIEW_DB}.DV_DWH_D_CUS_AS_OF_QTR_LU
				WHERE QTR_KEY ${QTR_CLAUSE}
			)LU
		SET CUS_HIST_KEY=LU.CUS_HIST_KEY
		WHERE TMP.CUS_KEY=LU.CUS_KEY
			AND TMP.QTR_KEY=LU.QTR_KEY;
		"
	
		run_query -d "${TEMP_DB}" -q "${UPDATE_SQL}" -m "Unable to Update Points Mth Records in temp table" 	
		print_msg "${TEMP_TABLE} loaded successfully"
		set_activity_count update
		audit_log 3
		set_bookmark "AFTER_UPDATE_TEMP_TABLE_1"
	fi
	
#	############### LOAD INTO TARGET TABLE ########################
#	if [[ ${BOOKMARK} = "AFTER_UPDATE_TEMP_TABLE_1" ]]
#	then
#		print_msg "Loading ${TARGET_TABLE}"
#		print_msg "Dropping Secondary Index for ${TARGET_TABLE} if exists"
#			
#		DROP_SQL="
#		SELECT 1 FROM DBC.INDICES WHERE DATABASENAME='${TARGET_DB}' AND TABLENAME='${TARGET_TABLE}' AND COLUMNNAME='CUS_HIST_KEY';
#		.if activitycount = 0 then GoTo ok
#		DROP INDEX (CUS_HIST_KEY) ON ${TARGET_DB}.${TARGET_TABLE};
#		.label ok
#		"
#		
#		run_query -d "${TARGET_DB}" -q "${DROP_SQL}" -m "Unable to drop secondary index for Target Table"
#		print_msg "${TARGET_TABLE} Secondary Index dropped successfully if exists"
#		print_msg "Deleting ${TARGET_TABLE}"
#		
#		DELETE_SQL="
#		DELETE FROM ${TARGET_DB}.${TARGET_TABLE} A
#		WHERE EXISTS (
#			SELECT 1
#			FROM ${TEMP_DB}.${TEMP_TABLE} B
#			WHERE A.CUS_KEY=B.CUS_KEY
#				AND A.WK_KEY>=B.WK_KEY
#		);
#		"
#	
#		set_audit_log_var
#		run_query -d "${TARGET_DB}" -q "${DELETE_SQL}" -m "Unable to Delete points records from target table"
#		print_msg "${TARGET_DB} deleted successfully"
#		set_activity_count delete
#		audit_log 3
#		set_bookmark "AFTER_DELETE_FROM_TARGET"
#	fi
	
#	if [[ ${BOOKMARK} = "AFTER_DELETE_FROM_TARGET" ]]
#	then
#		print_msg "Collecting Statistics for ${TARGET_TABLE}"
#	
#		STATS_SQL="
#		COLLECT STATS COLUMN(CUS_KEY,WK_KEY)
#			, COLUMN(CUS_KEY)
#			, COLUMN(WK_KEY)
#		ON ${TARGET_DB}.${TARGET_TABLE};
#		"
#	
#		run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
#		print_msg "${TARGET_TABLE} Statistics collected successfully"
#		set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE"
#	fi
	
	if [[ ${BOOKMARK} = "AFTER_UPDATE_TEMP_TABLE_1" ]]
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
			,QTR_KEY
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
			FROM ${VIEW_DB}.V_DWH_F_POINTS_CUS_QTR_A TGT
			WHERE SRC.CUS_KEY=TGT.CUS_KEY
				AND SRC.QTR_KEY=TGT.QTR_KEY
		);
		"
	
		run_query -d "${TARGET_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Points Records into target table"
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
		COLLECT STATS COLUMN(CUS_KEY,QTR_KEY)
			,COLUMN(CUS_KEY)
			,COLUMN(QTR_KEY)
		ON ${TARGET_DB}.${TARGET_TABLE};
		"
	
		run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
		print_msg "${TARGET_TABLE} Statistics collected successfully"
		set_bookmark "NONE"
	fi

done

print_msg "Creating Secondary Index for ${TARGET_TABLE}"

CREATE_SQL="
CREATE INDEX (CUS_HIST_KEY) ON ${TARGET_DB}.${TARGET_TABLE};
"

run_query -d "${TARGET_DB}" -q "${CREATE_SQL}" -m "Unable to create secondary index for Target Table"
print_msg "${TARGET_TABLE} Secondary Index created successfully"
set_bookmark "DONE"

script_successful
