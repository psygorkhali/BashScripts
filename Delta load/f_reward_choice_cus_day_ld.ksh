#!/bin/ksh

######################################################################
# Script : f_reward_choice_cus_day_ld.ksh
# Description : This script loads the reward choice Fact Data table 
#               DWH_F_REWARD_CHOICE_CUS_DAY_B using TEMP TABLE 
#				STG_F_REWARD_CHOICE_CUS_DAY_B.
# Modifications
# 12/24/2020  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
export DIM_CLOSABLE=0
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="STG_F_RWD_CHOICE_CUS_DAY_B"
TEMP0_TABLE="TMP0_F_RWD_CHOICE_CUS_DAY_B"
TEMP_TABLE="TMP_F_RWD_CHOICE_CUS_DAY_B"
TARGET_TABLE="DWH_F_RWD_CHOICE_CUS_DAY_B"

FACT_KEYS='CUS_KEY 
RWD_DEF_KEY'

OTHER_FIELDS='
REWARD_SET_DT
NEXT_REWARD_SET_DT
OPT_IN_FLG
BATCH_ID
'

FACT_FIELDS=''

###### SET VARIABLES ######################################
set_variables
set_dimension_variable
print_msg "${SCRIPT_NAME} Started"
start_script

ARCHIVE_DIR="${DATA_DIR}/customer/data_archive/"
DATA_DIR="${DATA_DIR}/customer/rewardchoice/"
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
	)
	SELECT 
	CUS.CUS_KEY, 
	RWDDEF.RWD_DEF_KEY,
	SRC.REWARD_SET_DT,
	COALESCE(
			MIN(SRC.REWARD_SET_DT) OVER (
				PARTITION BY CUS_KEY, RWD_DEF_KEY
				ORDER BY SRC.REWARD_SET_DT ROWS BETWEEN 1 FOLLOWING 
					AND 1 FOLLOWING
			), 
			CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD')
		) AS NEXT_REWARD_SET_DT,
	'Y',
	SRC.BATCH_ID
	FROM ${SRC_DB}.${SOURCE_TABLE} SRC
	INNER JOIN ${VIEW_DB}.V_DWH_D_CUS_LU CUS
	ON SRC.CUS_ID=CUS.CUS_ID
	INNER JOIN ${TARGET_DB}.DWH_D_RWD_DEF_LU RWDDEF
	ON RWDDEF.RWD_DEF_ID=SRC.RWD_ID
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for rewards choice into first temp table"
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
	COLLECT STATS COLUMN(CUS_KEY,RWD_DEF_KEY)
		,COLUMN(CUS_KEY)
		,COLUMN(RWD_DEF_KEY)
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
		,RWD_DEF_KEY
		,OPT_IN_FLG
		,EFF_FROM_DT
		,EFF_TO_DT
	)
	SELECT 
		 CUS_KEY
		,RWD_DEF_KEY
		,OPT_IN_FLG
		,BEGIN(A.PD) EFF_FROM_DT
		,END(A.PD) EFF_TO_DT
	FROM (
		SELECT NORMALIZE 
		CUS_KEY,
		RWD_DEF_KEY,
		OPT_IN_FLG,
		PERIOD(REWARD_SET_DT, NEXT_REWARD_SET_DT) PD
		FROM ${TEMP_DB}.${TEMP0_TABLE}
		) A;
	"

	run_query -d "$TEMP_DB" -q "$INSERT_SQL" -m "Unable to Insert Records for rewards into second temp table"
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
	COLLECT STATS COLUMN(CUS_KEY,EFF_FROM_DT)
		,COLUMN(CUS_KEY)
		,COLUMN(RWD_DEF_KEY)
		,COLUMN(EFF_FROM_DT)
	ON ${TEMP_DB}.${TEMP_TABLE};
	"

	run_query -d "${TEMP_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Temp Table"
	print_msg "${TEMP_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_SECOND_TEMP_TABLE"
fi

if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_SECOND_TEMP_TABLE" ]]
then
	print_msg "Updating ${TARGET_TABLE}"

	UPDATE_SQL="
	 UPDATE TGT
     FROM ${TARGET_DB}.${TARGET_TABLE} AS TGT, ${TEMP_DB}.${TEMP_TABLE} AS SRC
			SET 
					EFF_TO_DT=SRC.EFF_FROM_DT,
					OPT_IN_FLG='N',
					RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
		WHERE TGT.CUS_KEY=SRC.CUS_KEY
			AND TGT.RWD_DEF_KEY=SRC.RWD_DEF_KEY;
		"

	run_query -d "${TARGET_DB}" -q "${UPDATE_SQL}" -m "Unable to update Records into target table"
	print_msg "${TARGET_DB} updated successfully"
	set_activity_count update
	audit_log 3
	set_bookmark "AFTER_UPDATE_INTO_TARGET"
	print_msg "${TARGET_TABLE} Update Complete"
fi



################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_UPDATE_INTO_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,EFF_FROM_DT)
		,COLUMN(RWD_DEF_KEY)
		,COLUMN(EFF_FROM_DT)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"

	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_FOR_UPDATE_TARGET_TABLE"
fi

if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_FOR_UPDATE_TARGET_TABLE" ]]
then
	print_msg "Inserting ${TARGET_TABLE}"

	INSERT_SQL="
	INSERT INTO ${TARGET_DB}.${TARGET_TABLE} (
		 CUS_KEY
		,EFF_FROM_DT
		,EFF_TO_DT
		,OPT_IN_FLG
		,RWD_DEF_KEY
		,RCD_INS_TS
		,RCD_UPD_TS
	)
	SELECT CUS_KEY
		,EFF_FROM_DT
		,EFF_TO_DT
		,OPT_IN_FLG
		,RWD_DEF_KEY
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
		,$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
	FROM ${TEMP_DB}.${TEMP_TABLE} SRC
		WHERE NOT EXISTS (
			SELECT 1
			FROM ${VIEW_DB}.V_DWH_F_RWD_CHOICE_CUS_DAY_B TGT
			WHERE SRC.CUS_KEY=TGT.CUS_KEY AND SRC.RWD_DEF_KEY=TGT.RWD_DEF_KEY);
		"

	run_query -d "${TARGET_DB}" -q "${INSERT_SQL}" -m "Unable to insert Records into target table"
	print_msg "${TARGET_DB} inserted successfully"
	set_activity_count insert
	audit_log 3
	set_bookmark "AFTER_INSERT_INTO_TARGET"
	print_msg "${TARGET_TABLE} Insert Complete"
fi



################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_INTO_TARGET" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"

	STATS_SQL="
	COLLECT STATS COLUMN(CUS_KEY,RWD_DEF_KEY)
		,COLUMN(CUS_KEY)
		,COLUMN(EFF_FROM_DT)
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
