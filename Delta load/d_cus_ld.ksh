#!/bin/ksh

######################################################################
# Script : d_cus_ld.ksh
# Description : This script loads the Customer table DWH_D_CUS_LU 
#               using SOURCE TABLE STG_D_CUS_LU.
# Modifications
# 02/02/2019  : Logic  : Initial Script
######################################################################
export SCRIPT_NAME=$(basename ${0%%.ksh})
export DIM_CLOSABLE=0
. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh

##################################################
SOURCE_TABLE="STG_D_CUS_LU"
TEMP0_TABLE="TMP0_D_CUS_LU"
TEMP_TABLE="TMP_D_CUS_LU"
TARGET_TABLE="DWH_D_CUS_LU"

DIM_IDNT='CUS_ID'
DIM_KEY='CUS_KEY'

TEMP_TABLE_COLUMN='HHLD_KEY
FIRST_NAME
FIRST_NAME_SCRBD
LAST_NAME
LAST_NAME_SCRBD
ADDRESS1
ADDRESS1_SCRBD
ADDRESS2
ADDRESS2_SCRBD
CITY
CITY_SCRBD
STATE_CDE
STATE_SCRBD
ZIP_CDE
ZIP_CODE_SCRBD
ZIP4_CDE
ZIP4_CODE_SCRBD
ZIP_FULL_CDE_SCRBD
NON_US_POSTAL_CDE
COUNTRY_CDE
COUNTRY_CDE_SCRBD
NON_US_CUS_FLG
IS_ADDRESS_VALID_FLG
IS_ADDRESS_LIKELY_VALID_FLG
IS_EMAIL_VALID_FLG
IS_PHONE_FOR_SMS_VALID_FLG
PHONE_NUM
PHONE_TYP
EMAIL_ADDRESS
GENDER_CDE
DIRECT_MAIL_CONSENT_FLG
EMAIL_CONSENT_FLG
SMS_CONSENT_FLG
BIRTH_DT
AGE
AGE_GRP_CDE
AGE_QUALIFIER
DECEASED_FLG
ADDRESS_IS_PRISON_FLG
ADD_DT
INTRODCTN_DT
LATITUDE
LONGITUDE
CLOSEST_LOC_KEY
DIST_TO_CLOSEST_LOC
SECOND_CLOSEST_LOC_KEY
DIST_TO_SEC_CLOSEST_LOC
PREFERRED_LOC_KEY
DIST_TO_PREFERRED_LOC
FIRST_TXN_DT
FIRST_TXN_LOC_KEY
BRAND_FIRST_TXN_DT
ADS_DONOT_STATMNT_INS_IND
ADS_DONOT_SELL_NAME_IND
ADS_SPAM_IND
ADS_EMAIL_CHANGE_DT
ADS_RTRN_MAIL_FLG
IS_DM_MARKETABLE_FLG
IS_EM_MARKETABLE_FLG
IS_SMS_MARKETABLE_FLG
RCD_TYP
EMAIL_CONSENT_DT
SMS_CONSENT_DT
DIRECT_MAIL_CONSENT_DT
LYLTY_ID
HAS_PLCC_FLG
IS_LYLTY_MEMBER_FLG
IS_ALIST_FLG
LYLTY_TIER
LYLTY_MEMBER_STTS
LYLTY_ACCT_OPEN_DT
LYLTY_ACCT_CLOSE_DT
LYLTY_ACCT_ENROLL_DT
NEXT_CONVRSN_STTS
NCOA_LAST_CNG_DT
LYLTY_ENRLMNT_LOC_KEY
LYLTY_ENRLMNT_SRC_KEY
PLCC_OPEN_DT
PLCC_CLOSE_DT
PRIMARY_LOC_KEY
SECONDARY_LOC_KEY
LAST_TXN_DT
LAST_STR_PURCH_DT
LAST_WEB_PURCH_DT
GNDR_SHPNG_PREF_QTY
GNDR_SHPNG_PREF_AMT
PREFERRED_CHNL
DOMINANT_SHPNG_CHNL
LAST_BROWSE_DT
CS_AGENT_KEY
LOC_ASSOCIATE_KEY
EMP_AGG_KEY
REGSTRTN_DEVICE_TYP_CDE_KEY
REGSTRTN_DEVICE_TYP_ID
REGSTRTN_CHNL_SUB_TYP_ID
REGSTRTN_CHNL_ID
TERMNTN_REASN
LYLTY_ENRLMNT_SRC
PFCOM_FLG
PFCOM_DT
PFCOM_LOC_KEY
PFCOM_ASSOCIATE_KEY
PFCOM_CS_AGENT_KEY
PFCOM_EMP_AGG_KEY
PFCOM_SRC
PFCOM_DEVICE_TYP_CDE_KEY
PFCOM_DEVICE_TYP_ID
PFCOM_CHNL_SUB_TYP_ID
PFCOM_CHNL_ID
PFCOM_RWD_FLG
PFUPD_DT
YRLY_PFUPD_DT
PFUPD_LOC_KEY
PFUPD_ASSOCIATE_KEY
PFUPD_CS_AGENT_KEY
PFUPD_EMP_AGG_KEY
PFUPD_SRC
PFUPD_DEVICE_TYP_CDE_KEY
PFUPD_DEVICE_TYP_ID
PFUPD_CHNL_SUB_TYP_ID
PFUPD_CHNL_ID
YRLY_PFUPD_DEVICE_TYP_CDE_KEY
YRLY_PFUPD_DEVICE_TYP_ID
YRLY_PFUPD_CHNL_SUB_TYP_ID
YRLY_PFUPD_CHNL_ID
SIGNUP_TYP
LYLTY_TIER_KEY
TIER_FROM_DT
TIER_TO_DT
TIER_MOVE_REASN
EMP_KEY
IS_EMP_FLG'

####### SET VARIABLES ######################################
set_variables
print_msg "${SCRIPT_NAME} Started"
start_script

ARCHIVE_DIR="${DATA_DIR}/customer/data_archive/"
DATA_DIR="${DATA_DIR}/customer/customer/"
PREFIX_FILE_NAME="`(echo "${SOURCE_TABLE#STG_}_VALIDATION" | tr [:upper:] [:lower:])`"
FILE_LIST=$(ls -1 ${DATA_DIR}| grep -v ${PREFIX_FILE_NAME})
FILE_LIST=(${FILE_LIST[@]})
FILE_COUNT=${#FILE_LIST[@]}

#if [[ ${FILE_COUNT} != 1  ]]
#then
#	print_msg "Unexpected number of data files encountered: ${FILE_COUNT}"
#	print_msg "Expected 1 data file for processing at a time"
#	chk_err -r 1 -m "Expected 1 data file for processing at a time"
#fi

DATA_FILE=${DATA_DIR}${FILE_LIST[0]}

####### LOAD INTO STAGING TABLE ##############
if [[ ${BOOKMARK} = "NONE" ]]
then
	print_msg "Dropping Secondary Index for ${SOURCE_TABLE} if exists"
	
	DROP_SQL="
	SELECT 1 FROM DBC.INDICES WHERE DATABASENAME='${SRC_DB}' AND TABLENAME='${SOURCE_TABLE}' AND COLUMNNAME='HHLD_ID';
	.if activitycount = 0 then GoTo ok
	DROP INDEX (HHLD_ID) ON ${SRC_DB}.${SOURCE_TABLE};
	.label ok
	"
	
	run_query -d "${SRC_DB}" -q "${DROP_SQL}" -m "Unable to drop secondary index for Stage Table"
	print_msg "${SOURCE_TABLE} Secondary Index dropped successfully if exists"
#    truncate_staging_table	
#	load_data
	print_msg "Creating Secondary Index for ${SOURCE_TABLE}"
	
	CREATE_SQL="
	CREATE INDEX (HHLD_ID) ON ${SRC_DB}.${SOURCE_TABLE};
	"
	
	run_query -d "${SRC_DB}" -q "${CREATE_SQL}" -m "Unable to create secondary index for Stage Table"
	print_msg "${SOURCE_TABLE} Secondary Index created successfully"
	set_bookmark "AFTER_STG_LOAD"
fi

###### COLLECT STATS FOR STAGING TABLE ##############
if [[ ${BOOKMARK} = "AFTER_STG_LOAD" ]]
then
	print_msg "Collecting Statistics for ${SOURCE_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(CUS_ID, INGEST_DT)
		,COLUMN(HHLD_ID)
	ON ${SRC_DB}.${SOURCE_TABLE};
	"
	
	run_query -d "${SRC_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Stage Table"
	print_msg "${SOURCE_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_STG_COLLECT_STATS"
fi

####### LOAD INTO TEMPORARY0 TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_STG_COLLECT_STATS" ]]
then
	print_msg "Load into Temporary0 Table Started"
	print_msg "Truncate Temporary0 Table"
	truncate_table -d "${TEMP_DB}" -t "${TEMP0_TABLE}"
	print_msg "${TEMP0_TABLE} truncated successfully"
	print_msg "Loading ${TEMP0_TABLE}"
	
	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP0_TABLE} ( 
		${DIM_IDNT}
		,${TEMP_TABLE_COLUMN_LIST}
		,HHLD_HIST_KEY
		,HHLD_ID	
		,CLOSEST_LOC_ID
		,SECOND_CLOSEST_LOC_ID
		,PREFERRED_LOC_ID
		,FIRST_TXN_LOC_ID
		,LYLTY_ENRLMNT_LOC_ID
		,LYLTY_ENRLMNT_SRC_ID
		,PRIMARY_LOC_ID
		,SECONDARY_LOC_ID
		,CS_AGENT_ID
		,LOC_ASSOCIATE_ID
		,REGSTRTN_DEVICE_TYP_CDE_ID
		,PFCOM_LOC_ID
		,PFCOM_ASSOCIATE_ID
		,PFCOM_CS_AGENT_ID
		,PFCOM_DEVICE_TYP_CDE_ID
		,PFUPD_LOC_ID
		,PFUPD_ASSOCIATE_ID
		,PFUPD_CS_AGENT_ID
		,PFUPD_DEVICE_TYP_CDE_ID
		,YRLY_PFUPD_DEVICE_TYP_CDE_ID
		,LYLTY_TIER_ID
		,EMP_ID
		,INGEST_DT
		,NEXT_INGEST_DT
	)
	SELECT SRC.CUS_ID
		,COALESCE(HHLD.HHLD_KEY,-1)
		,SRC.FIRST_NAME
		,SRC.FIRST_NAME_SCRBD
		,SRC.LAST_NAME
		,SRC.LAST_NAME_SCRBD
		,SRC.ADDRESS1
		,SRC.ADDRESS1_SCRBD
		,SRC.ADDRESS2
		,SRC.ADDRESS2_SCRBD
		,SRC.CITY
		,SRC.CITY_SCRBD
		,SRC.STATE_CDE
		,SRC.STATE_SCRBD
		,SRC.ZIP_CDE
		,SRC.ZIP_CODE_SCRBD
		,SRC.ZIP4_CDE
		,SRC.ZIP4_CODE_SCRBD
		,SRC.ZIP_FULL_CDE_SCRBD
		,SRC.NON_US_POSTAL_CDE
		,SRC.COUNTRY_CDE
		,SRC.COUNTRY_CDE_SCRBD
		,CASE 
			WHEN TRIM(SRC.COUNTRY_CDE)='US' OR TRIM(SRC.COUNTRY_CDE)='USA'
				THEN 0
				ELSE 1
			END NON_US_CUS_FLG
		,SRC.IS_ADDRESS_VALID_FLG
		,SRC.IS_ADDRESS_LIKELY_VALID_FLG
		,SRC.IS_EMAIL_VALID_FLG
		,SRC.IS_PHONE_FOR_SMS_VALID_FLG
		,SRC.PHONE_NUM
		,SRC.PHONE_TYP
		,SRC.EMAIL_ADDRESS
		,SRC.GENDER_CDE
		,SRC.DIRECT_MAIL_CONSENT_FLG
		,SRC.EMAIL_CONSENT_FLG
		,SRC.SMS_CONSENT_FLG
		,SRC.BIRTH_DT
		,SRC.AGE
		,AGEGRP.AGE_STRAT_CDE AGE_GRP_CDE
		,SRC.AGE_QUALIFIER
		,SRC.DECEASED_FLG
		,SRC.ADDRESS_IS_PRISON_FLG
		,SRC.ADD_DT
		,SRC.INTRODCTN_DT
		,SRC.LATITUDE
		,SRC.LONGITUDE
		,COALESCE(CL.LOC_KEY,-1) CLOSEST_LOC_KEY
		,SRC.DIST_TO_CLOSEST_LOC
		,COALESCE(SCL.LOC_KEY,-1) SECONDARY_LOC_KEY
		,SRC.DIST_TO_SEC_CLOSEST_LOC
		,COALESCE(PL.LOC_KEY,-1) PREFERRED_LOC_KEY
		,SRC.DIST_TO_PREFERRED_LOC
		,SRC.FIRST_TXN_DT
		,COALESCE(FTL.LOC_KEY,-1) FIRST_TXN_LOC_KEY
		,SRC.BRAND_FIRST_TXN_DT
		,SRC.ADS_DONOT_STATMNT_INS_IND
		,SRC.ADS_DONOT_SELL_NAME_IND
		,SRC.ADS_SPAM_IND
		,SRC.ADS_EMAIL_CHANGE_DT
		,SRC.ADS_RTRN_MAIL_FLG
		,SRC.IS_DM_MARKETABLE_FLG
		,SRC.IS_EM_MARKETABLE_FLG
		,SRC.IS_SMS_MARKETABLE_FLG
		,SRC.RCD_TYP
		,SRC.EMAIL_CONSENT_DT
		,SRC.SMS_CONSENT_DT
		,SRC.DIRECT_MAIL_CONSENT_DT
		,SRC.LYLTY_ID
		,SRC.HAS_PLCC_FLG
		,SRC.IS_LYLTY_MEMBER_FLG
		,CASE 
			WHEN SRC.LYLTY_TIER=30
				THEN 1
				ELSE 0
			END IS_ALIST_FLG
		,SRC.LYLTY_TIER 
		,SRC.LYLTY_MEMBER_STTS
		,SRC.LYLTY_ACCT_OPEN_DT
		,SRC.LYLTY_ACCT_CLOSE_DT
		,SRC.LYLTY_ACCT_ENROLL_DT
		,SRC.NEXT_CONVRSN_STTS
		,SRC.NCOA_LAST_CNG_DT
		,COALESCE(LYLLOC.LOC_KEY,-1) LYLTY_ENRLMNT_LOC_KEY
		,COALESCE(LYLSRC.LYLTY_ENRLMNT_SRC_KEY,-1) LYLTY_ENRLMNT_SRC_KEY
		,SRC.PLCC_OPEN_DT
		,SRC.PLCC_CLOSE_DT
		,COALESCE(PRILOC.LOC_KEY,-1) PRIMARY_LOC_KEY
		,COALESCE(SECLOC.LOC_KEY,-1) SECONDARY_LOC_KEY
		,SRC.LAST_TXN_DT
		,SRC.LAST_STR_PURCH_DT
		,SRC.LAST_WEB_PURCH_DT
		,SRC.GNDR_SHPNG_PREF_QTY
		,SRC.GNDR_SHPNG_PREF_AMT
		,SRC.PREFERRED_CHNL
		,SRC.DOMINANT_SHPNG_CHNL
		,SRC.LAST_BROWSE_DT
		,COALESCE(CSA_NEW_REG.CS_AGENT_KEY,-1) CS_AGENT_KEY
		,COALESCE(EMP_NEW_REG.EMP_KEY,-1) LOC_ASSOCIATE_KEY
		,COALESCE(EMP_AGG_NEW_REG.EMP_AGG_KEY,-1) EMP_AGG_KEY
		,COALESCE(REG_DEV_TYP.DEVICE_TYP_CDE_KEY,-1) REGSTRTN_DEVICE_TYP_CDE_KEY
		,COALESCE(REG_DEV_TYP.DEVICE_TYP_ID,-1) REGSTRTN_DEVICE_TYP_ID
		,COALESCE(REG_DEV_TYP.CHNL_SUB_TYP_ID,-1) REGSTRTN_CHNL_SUB_TYP_ID
		,COALESCE(REG_DEV_TYP.CHNL_ID,-1) REGSTRTN_CHNL_ID
		,SRC.TERMNTN_REASN
		,SRC.LYLTY_ENRLMNT_SRC
		,SRC.PFCOM_FLG
		,SRC.PFCOM_DT
		,COALESCE(LOC_PFCOM.LOC_KEY,-1) PFCOM_LOC_KEY
		,COALESCE(EMP_PFCOM.EMP_KEY,-1) PFCOM_ASSOCIATE_KEY
		,COALESCE(CSA_PFCOM.CS_AGENT_KEY,-1) PFCOM_CS_AGENT_KEY
		,COALESCE(EMP_AGG_PFCOM.EMP_AGG_KEY,-1) PFCOM_EMP_AGG_KEY
		,SRC.PFCOM_SRC
		,COALESCE(PFCOM_DEV_TYP.DEVICE_TYP_CDE_KEY,-1) PFCOM_DEVICE_TYP_CDE_KEY
		,COALESCE(PFCOM_DEV_TYP.DEVICE_TYP_ID,-1) PFCOM_DEVICE_TYP_ID
		,COALESCE(PFCOM_DEV_TYP.CHNL_SUB_TYP_ID,-1) PFCOM_CHNL_SUB_TYP_ID
		,COALESCE(PFCOM_DEV_TYP.CHNL_ID,-1) PFCOM_CHNL_ID
		,SRC.PFCOM_RWD_FLG
		,SRC.PFUPD_DT
		,SRC.YRLY_PFUPD_DT
		,COALESCE(LOC_PFUPD.LOC_KEY,-1) PFUPD_LOC_KEY
		,COALESCE(EMP_PFUPD.EMP_KEY,-1) PFUPD_ASSOCIATE_KEY
		,COALESCE(CSA_PFUPD.CS_AGENT_KEY,-1) PFUPD_CS_AGENT_KEY
		,COALESCE(EMP_AGG_PFUPD.EMP_AGG_KEY,-1) PFUPD_EMP_AGG_KEY
		,SRC.PFUPD_SRC
		,COALESCE(PFUPD_DEV_TYP.DEVICE_TYP_CDE_KEY,-1) PFUPD_DEVICE_TYP_CDE_KEY
		,COALESCE(PFUPD_DEV_TYP.DEVICE_TYP_ID,-1) PFUPD_DEVICE_TYP_ID
		,COALESCE(PFUPD_DEV_TYP.CHNL_SUB_TYP_ID,-1) PFUPD_CHNL_SUB_TYP_ID
		,COALESCE(PFUPD_DEV_TYP.CHNL_ID,-1) PFUPD_CHNL_ID
		,COALESCE(YRLY_PFUPD_DEV_TYP.DEVICE_TYP_CDE_KEY,-1) YRLY_PFUPD_DEVICE_TYP_CDE_KEY
		,COALESCE(YRLY_PFUPD_DEV_TYP.DEVICE_TYP_ID,-1) YRLY_PFUPD_DEVICE_TYP_ID
		,COALESCE(YRLY_PFUPD_DEV_TYP.CHNL_SUB_TYP_ID,-1) YRLY_PFUPD_CHNL_SUB_TYP_ID
		,COALESCE(YRLY_PFUPD_DEV_TYP.CHNL_ID,-1) YRLY_PFUPD_CHNL_ID
		,SRC.SIGNUP_TYP
		,COALESCE(LYLTY_TIER.TIER_KEY,-1) LYLTY_TIER_KEY
		,SRC.TIER_FROM_DT
		,SRC.TIER_TO_DT
		,SRC.TIER_MOVE_REASN
		,COALESCE(EMP.EMP_KEY,-1) EMP_KEY
		,IS_EMP_FLG
		,COALESCE(HHLD_HIST.HHLD_HIST_KEY, -1) HHLD_HIST_KEY
		,SRC.HHLD_ID
		,TRIM(SRC.CLOSEST_LOC_ID)
		,TRIM(SRC.SECOND_CLOSEST_LOC_ID)
		,TRIM(SRC.PREFERRED_LOC_ID)
		,TRIM(SRC.FIRST_TXN_LOC_ID)
		,TRIM(SRC.LYLTY_ENRLMNT_LOC_ID)
		,SRC.LYLTY_ENRLMNT_SRC_ID
		,TRIM(SRC.PRIMARY_LOC_ID)
		,TRIM(SRC.SECONDARY_LOC_ID)
		,SRC.CS_AGENT_ID
		,SRC.LOC_ASSOCIATE_ID
		,SRC.REGSTRTN_DEVICE_TYP_CDE_ID
		,SRC.PFCOM_LOC_ID
		,SRC.PFCOM_ASSOCIATE_ID
		,SRC.PFCOM_CS_AGENT_ID
		,SRC.PFCOM_DEVICE_TYP_CDE_ID
		,SRC.PFUPD_LOC_ID
		,SRC.PFUPD_ASSOCIATE_ID
		,SRC.PFUPD_CS_AGENT_ID
		,SRC.PFUPD_DEVICE_TYP_CDE_ID
		,SRC.YRLY_PFUPD_DEVICE_TYP_CDE_ID
		,SRC.LYLTY_TIER_ID		
		,SRC.EMP_ID
		,SRC.INGEST_DT
		,COALESCE(
			MIN(SRC.INGEST_DT) OVER (
				PARTITION BY SRC.CUS_ID 
				ORDER BY SRC.INGEST_DT ROWS BETWEEN 1 FOLLOWING 
					AND 1 FOLLOWING
			), 
			CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD')
		) AS NEXT_INGEST_DT		
	FROM (
		SELECT A.* 
			,TRIM(COALESCE('CSA-'||CS_AGENT_ID,'EMP-'||LOC_ASSOCIATE_ID)) EMP_AGG_ID
			,TRIM(COALESCE('CSA-'||PFCOM_CS_AGENT_ID,'EMP-'||PFCOM_ASSOCIATE_ID)) PFCOM_EMP_AGG_ID
			,TRIM(COALESCE('CSA-'||PFUPD_ASSOCIATE_ID,'EMP-'||PFUPD_ASSOCIATE_ID)) PFUPD_EMP_AGG_ID
		FROM ${SRC_DB}.${SOURCE_TABLE} A 
		WHERE CUS_ID<>-1
	) SRC
	LEFT OUTER JOIN (
		SELECT HHLD_KEY
			,HHLD_ID 
		FROM ${VIEW_DB}.V_DWH_D_CUS_HHLD_LU
	) HHLD ON SRC.HHLD_ID=HHLD.HHLD_ID
	LEFT OUTER JOIN (
		SELECT HHLD_HIST_KEY 
			,HHLD_ID
			,EFF_FROM_DT
			,EFF_TO_DT
		FROM ${VIEW_DB}.V_DWH_D_CUS_HHLD_HIST_LU
	) HHLD_HIST ON SRC.HHLD_ID=HHLD_HIST.HHLD_ID 
		AND SRC.INGEST_DT BETWEEN HHLD_HIST.EFF_FROM_DT AND HHLD_HIST.EFF_TO_DT
	LEFT OUTER JOIN ${VIEW_DB}.DV_DWH_D_CUS_AGE_STRAT_CDE_LU AGEGRP 
		ON SRC.AGE BETWEEN AGEGRP.STRAT_MIN_VAL AND AGEGRP.STRAT_MAX_VAL
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	)CL	ON TRIM(SRC.CLOSEST_LOC_ID) = CL.LOC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) SCL ON TRIM(SRC.SECOND_CLOSEST_LOC_ID) = SCL.LOC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) PL ON TRIM(SRC.PREFERRED_LOC_ID) = PL.LOC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) FTL ON TRIM(SRC.FIRST_TXN_LOC_ID) = FTL.LOC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) LYLLOC ON TRIM(SRC.LYLTY_ENRLMNT_LOC_ID) = LYLLOC.LOC_ID
	LEFT OUTER JOIN (
		SELECT LYLTY_ENRLMNT_SRC_KEY
			,LYLTY_ENRLMNT_SRC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_LYLTY_ENRLMNT_SRC_LU
	) LYLSRC ON SRC.LYLTY_ENRLMNT_SRC_ID = LYLSRC.LYLTY_ENRLMNT_SRC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) PRILOC ON TRIM(SRC.PRIMARY_LOC_ID) = PRILOC.LOC_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) SECLOC ON TRIM(SRC.SECONDARY_LOC_ID) = SECLOC.LOC_ID
	LEFT OUTER JOIN (
		SELECT CS_AGENT_KEY
			,CS_AGENT_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_CS_AGENT_LU
	) CSA_NEW_REG ON SRC.CS_AGENT_ID=CSA_NEW_REG.CS_AGENT_ID
	LEFT OUTER JOIN (
		SELECT EMP_KEY
			,EMP_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_LU
	) EMP_NEW_REG ON SRC.LOC_ASSOCIATE_ID=EMP_NEW_REG.EMP_ID	
	LEFT OUTER JOIN (
		SELECT EMP_AGG_KEY
			,EMP_AGG_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_AGG_LU
	) EMP_AGG_NEW_REG ON SRC.EMP_AGG_ID=EMP_AGG_NEW_REG.EMP_AGG_ID
	LEFT OUTER JOIN (
		SELECT DEVICE_TYP_CDE_KEY
			,DEVICE_TYP_CDE_ID
			,DEVICE_TYP_ID
			,CHNL_SUB_TYP_ID
			,CHNL_ID		
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_DEVICE_TYP_LU
	) REG_DEV_TYP ON SRC.REGSTRTN_DEVICE_TYP_CDE_ID=REG_DEV_TYP.DEVICE_TYP_CDE_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) LOC_PFCOM	ON SRC.PFCOM_LOC_ID=LOC_PFCOM.LOC_ID
	LEFT OUTER JOIN (
		SELECT EMP_KEY
			,EMP_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_LU
	) EMP_PFCOM ON SRC.PFCOM_ASSOCIATE_ID=EMP_PFCOM.EMP_ID
	LEFT OUTER JOIN (
		SELECT CS_AGENT_KEY
			,CS_AGENT_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_CS_AGENT_LU
	) CSA_PFCOM ON SRC.PFCOM_CS_AGENT_ID=CSA_PFCOM.CS_AGENT_ID
	LEFT OUTER JOIN (
		SELECT EMP_AGG_KEY
			,EMP_AGG_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_AGG_LU
	) EMP_AGG_PFCOM ON SRC.PFCOM_EMP_AGG_ID=EMP_AGG_PFCOM.EMP_AGG_ID
	LEFT OUTER JOIN (
		SELECT DEVICE_TYP_CDE_KEY
			,DEVICE_TYP_CDE_ID
			,DEVICE_TYP_ID
			,CHNL_SUB_TYP_ID
			,CHNL_ID		
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_DEVICE_TYP_LU
	) PFCOM_DEV_TYP ON SRC.PFCOM_DEVICE_TYP_CDE_ID=PFCOM_DEV_TYP.DEVICE_TYP_CDE_ID
	LEFT OUTER JOIN (
		SELECT LOC_KEY
			,LOC_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU
	) LOC_PFUPD ON SRC.PFUPD_LOC_ID=LOC_PFUPD.LOC_ID
	LEFT OUTER JOIN (
		SELECT EMP_KEY
			,EMP_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_LU
	) EMP_PFUPD ON SRC.PFUPD_ASSOCIATE_ID=EMP_PFUPD.EMP_ID
	LEFT OUTER JOIN (
		SELECT CS_AGENT_KEY
			,CS_AGENT_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_CS_AGENT_LU
	) CSA_PFUPD	ON SRC.PFUPD_CS_AGENT_ID=CSA_PFUPD.CS_AGENT_ID
	LEFT OUTER JOIN (
		SELECT EMP_AGG_KEY
			,EMP_AGG_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_AGG_LU
	) EMP_AGG_PFUPD ON SRC.PFUPD_EMP_AGG_ID=EMP_AGG_PFUPD.EMP_AGG_ID
	LEFT OUTER JOIN (
		SELECT DEVICE_TYP_CDE_KEY
			,DEVICE_TYP_CDE_ID
			,DEVICE_TYP_ID
			,CHNL_SUB_TYP_ID
			,CHNL_ID		
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_DEVICE_TYP_LU
	) PFUPD_DEV_TYP ON SRC.PFUPD_DEVICE_TYP_CDE_ID=PFUPD_DEV_TYP.DEVICE_TYP_CDE_ID
	LEFT OUTER JOIN (
		SELECT DEVICE_TYP_CDE_KEY
			,DEVICE_TYP_CDE_ID
			,DEVICE_TYP_ID
			,CHNL_SUB_TYP_ID
			,CHNL_ID		
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_DEVICE_TYP_LU
	) YRLY_PFUPD_DEV_TYP ON SRC.PFUPD_DEVICE_TYP_CDE_ID=YRLY_PFUPD_DEV_TYP.DEVICE_TYP_CDE_ID	
	LEFT OUTER JOIN (
		SELECT TIER_KEY
			,TIER_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_CUS_TIER_LU
	) LYLTY_TIER ON SRC.LYLTY_TIER_ID=LYLTY_TIER.TIER_ID
	LEFT OUTER JOIN (
		SELECT EMP_KEY
			,EMP_ID
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_D_EMP_LU
	) EMP ON SRC.EMP_ID=EMP.EMP_ID;
	"

	run_query -d "${TEMP_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Records for Customer into temp0 table" 
	print_msg "${TEMP0_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${SRC_DB} ${SOURCE_TABLE} "*" ${TEMP0_TABLE}
	set_bookmark "AFTER_LOAD_TEMP0_TABLE"
fi

####### LOAD INTO TEMPORARY TABLE ##########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP0_TABLE" ]]
then
	print_msg "Load into Temporary Table Started"
	print_msg "Truncate Temporary Table"
	truncate_table -d "${TEMP_DB}" -t "${TEMP_TABLE}"
	print_msg "${TEMP_TABLE} truncated successfully"
	print_msg "Loading ${TEMP_TABLE}"
	
	INSERT_SQL="
	INSERT INTO ${TEMP_DB}.${TEMP_TABLE}
		( ${DIM_IDNT}
		,${TEMP_TABLE_COLUMN_LIST}						
		)
	SELECT ${DIM_IDNT}
		,${TEMP_TABLE_COLUMN_LIST}
	FROM ${TEMP_DB}.${TEMP0_TABLE} SRC
	QUALIFY SRC.INGEST_DT = MAX(SRC.INGEST_DT) 
		OVER (PARTITION BY SRC.CUS_ID);
	"

	run_query -d "${TEMP_DB}" -q "${INSERT_SQL}" -m "Unable to Insert Records for Customer into temp table" 
	print_msg "${TEMP_TABLE} loaded successfully"
	set_activity_count insert
	audit_log 2 ${TEMP_DB} ${TEMP0_TABLE} "*" ${TEMP_TABLE}
	set_bookmark "AFTER_LOAD_TEMP_TABLE"
fi

############### LOAD INTO TARGET TABLE ########################
if [[ ${BOOKMARK} = "AFTER_LOAD_TEMP_TABLE" ]]
then
	print_msg "Loading ${TARGET_TABLE}"
	update_using_temp_cdh
    set_bookmark "AFTER_UPDATE_USING_TEMP"
fi

if [[ ${BOOKMARK} = "AFTER_UPDATE_USING_TEMP" ]]
then
	insert_from_temp
	set_bookmark "AFTER_INSERT_FROM_TEMP"
	print_msg "${TARGET_TABLE} Load Complete"
fi

################ COLLECT STATS FOR TARGET TABLE ##################
if [[ ${BOOKMARK} = "AFTER_INSERT_FROM_TEMP" ]]
then
	print_msg "Collecting Statistics for ${TARGET_TABLE}"
	
	STATS_SQL="
	COLLECT STATS COLUMN(CUS_ID)
		, COLUMN(CUS_KEY)
	ON ${TARGET_DB}.${TARGET_TABLE};
	"
	
	run_query -d "${TARGET_DB}" -q "${STATS_SQL}" -m "Unable to collect statistics for Target Table"
	print_msg "${TARGET_TABLE} Statistics collected successfully"
	set_bookmark "AFTER_COLLECT_STATS_TARGET_TABLE"
fi

############### DATA FILE POST-PROCESS ########################
if [[ ${BOOKMARK} = "AFTER_COLLECT_STATS_TARGET_TABLE" ]]
then
	if [[ -f ${DATA_FILE} ]]
	then
		print_msg "Adding checksum to data file for archiving"
#		add_file_checksum
#		print_msg "Archiving data file from ${DATA_FILE} to ${ARCHIVE_DIR}"
#		mv -f ${DATA_FILE} ${ARCHIVE_DIR}
		set_bookmark "DONE"
	fi
fi

script_successful