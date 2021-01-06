#!/bin/ksh
#set -e
######################################################################
# Script : scheduler_functions.ksh                                   #
# Description : This library file contains all functions related to  #
#               scheduling the ETL Batch                             #
# Modifications                                                      #
# 1/18/2013  : Logic  : Initial Script								 #
# 9/23/2014	 : Updated function description                          #
# 2/24/2017  : Logic  : Added the scheduler functoin for ODS cyclic  #
######################################################################

#############################################################################################################
# Function Name : get_batch_id
# Description   : This function selects maximum value of BATCH_ID from DWH_C_BATCH_LOG table. 
# Value Returns : BATCH_ID
#############################################################################################################

function get_batch_id {
   print_msg "Getting Batch ID from DWH_C_BATCH_LOG"
      
   GET_BATCH_ID_SQL="SELECT $(DATATYPE_CONV 'COALESCE(MAX(BATCH_ID),0)' 'VARCHAR(20)') FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG WHERE MODULE_NAME='${MODULE_TYPE}'"
   
   BATCH_ID=$(get_result -d "${VIEW_DB}" -q "$GET_BATCH_ID_SQL" -m "Unable to get Batch ID")
    print_msg "The current Batch ID is : ${BATCH_ID}"
	echo ""
   export BATCH_ID
  
}

#############################################################################################################
# Function Name : get_job_id
# Description   : This function selects value of JOB_ID from DWH_C_BATCH_SCRIPTS table. 
# Value Returns : JOB_ID
#############################################################################################################

function get_job_id {

	print_msg "Getting Job Id from DWH_C_BATCH_SCRIPTS"
   GET_JOB_ID_SQL="SELECT TRIM(JOB_ID) FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_SCRIPTS WHERE LOWER(SCRIPT_NAME)=LOWER('${SCRIPT_NAME}')"
   
   JOB_ID=$(get_result -d "${VIEW_DB}" -q "$GET_JOB_ID_SQL" -m "Unable to get Job ID")
   print_msg "The current Job ID is : ${JOB_ID}"
   echo ""
   export JOB_ID
}

#############################################################################################################
# Function Name : get_module_type
# Description   : This function checks the module type of SCRIPT from DWH_C_BATCH_SCRIPTS table. 
# Value Returns : MODULE_TYPE
#############################################################################################################

function get_module_type {
   print_msg "Getting Module Type from DWH_C_BATCH_SCRIPTS"
      
   GET_MODULE_TYPE_SQL="SELECT MODULE_TYP FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_SCRIPTS WHERE LOWER(SCRIPT_NAME)=LOWER('${SCRIPT_NAME}')"

   MODULE_TYPE=$(get_result -d "${VIEW_DB}" -q "$GET_MODULE_TYPE_SQL" -m "Unable to get Module Type for $SCRIPT_NAME")
    print_msg "The current Module Type is : ${MODULE_TYPE}"
	echo ""
   export MODULE_TYPE
}

#############################################################################################################
# Function Name : get_module_load_type
# Description   : This function checks the module load type of SCRIPT from DWH_C_BATCH_SCRIPTS table. 
# Value Returns : MODULE_LOAD_TYPE
#############################################################################################################

function get_module_load_type {
   print_msg "Getting Module Load Type from DWH_C_BATCH_SCRIPTS"
      
   GET_MODULE_LOAD_TYPE_SQL="SELECT MODULE_LOAD_TYP FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_SCRIPTS WHERE LOWER(SCRIPT_NAME)=LOWER('${SCRIPT_NAME}')"
   
   MODULE_LOAD_TYPE=$(get_result -d "${VIEW_DB}" -q "$GET_MODULE_LOAD_TYPE_SQL" -m "Unable to get Module Load Type for $SCRIPT_NAME")
    print_msg "The current Module Load Type is : ${MODULE_LOAD_TYPE}"
	export MODULE_LOAD_TYPE
}

#############################################################################################################
# Function Name : check_last_batch_run
# Description   : This function checks the status of Last batch run from DWH_BATCH_C_LOG table
#############################################################################################################

function check_last_batch_run {

print_msg "Checking status of last batch run from DWH_C_BATCH_LOG"
CHECK_LAST_BATCH_RUN="SELECT CASE WHEN COUNT(*) = 0 
                                 THEN 1  -- ETL Batch has not been Started or has already Completed
                                 ELSE 0  -- ETL Batch has been Started and is Currently Running
                              END chk_batch
                      FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG 
                        WHERE BATCH_ID = '${BATCH_ID}'
                         AND UPPER(STATUS) = 'COMPLETE'"

RUN_SCRIPT=$(get_result -d "${VIEW_DB}" -q "$CHECK_LAST_BATCH_RUN" -m "Unable to check last batch status")

chk_err -r ${RUN_SCRIPT} -m "Previous Batch Not Completed. Fix errors and complete Previous batch first."

}

#############################################################################################################
# Function Name : start_script
# Description   : This function inserts/updates data into DWH_C_BATCH_LOG control table whenever a script is executed
#############################################################################################################

function start_script {
	######sending status mail #############
	send_script_run_mail 1
    
	print_msg "Getting current time-stamp for the job execution"
	GET_TS="SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))"
			
	GET_TIME_STAMP=$(get_result -d "$TARGET_DB" -q "$GET_TS" -m "Unable to CURRENT_TIMESTAMP")
	print_msg "The current time-stamp of the job = ${GET_TIME_STAMP}"
	echo ""
	export GET_TIME_STAMP

	print_msg "Inserting batch log"
	INSERT_BATCH_LOG_SQL="INSERT INTO ${TARGET_DB}.DWH_C_BATCH_LOG (
                        BATCH_ID 
    				   ,JOB_ID
    				   ,MODULE_NAME
    				   ,JOB_NAME
    				   ,BUSINESS_DATE
    				   ,START_TIMESTAMP
    				   ,END_TIMESTAMP
    				   ,STATUS
    				   ,ERROR_DETAIL
    				   ,BOOKMARK
    				   ,LOGFILE)
                   SELECT ${BATCH_ID}
                         ,${JOB_ID}
                         ,'${MODULE_TYPE}'
                        ,'${SCRIPT_NAME}'
    					 ,'${CURR_DAY}'
                         ,'${GET_TIME_STAMP}'
                         ,$(DATATYPE_CONV "'9999-09-09'" DATE 'YYYY-MM-DD')
                         ,'RUNNING'
                         ,'RUNNING'
                         ,'${BOOKMARK}'
                         ,'$LOG_FILE'
                   "
    
    run_query -d "$TARGET_DB" -q "$INSERT_BATCH_LOG_SQL" -m "Unable to start ${SCRIPT_NAME} log"
	
}

###################################################################################################
# Function Name : script_successful
# Description   : This function updates DWH_C_BATCH_LOG table if the script completes successfully
###################################################################################################	

 function script_successful {
	print_msg "Script Successful"
    UPDATE_BATCH_LOG_SQL="UPDATE ${TARGET_DB}.DWH_C_BATCH_LOG 
                     SET      
                            END_TIMESTAMP = CURRENT_TIMESTAMP
                            ,STATUS='COMPLETE'
							,JOB_ID='${JOB_ID}'
							,ERROR_DETAIL='COMPLETE'
							,BOOKMARK='COMPLETE'
                     WHERE BATCH_ID='${BATCH_ID}'
							AND JOB_NAME='${SCRIPT_NAME}'
							AND START_TIMESTAMP='${GET_TIME_STAMP}'"                    
    
    run_query -d "$TARGET_DB" -q "$UPDATE_BATCH_LOG_SQL" -m "Unable to update ${SCRIPT_NAME} log"
	
	
	
	if [[ -f ${DATA_FILE} ]]
	then
		if [[ ${SEED_FLAG} = 0 ]]
		then
			print_msg "Adding checksum for Archive"
			add_file_checksum
		fi
		DATA=$(basename ${DATA_FILE})
		mv -f ${DATA_DIR}/${DATA} ${ARCHIVE_DIR}/${DATA}
    fi
	
	
	#########remove errror file if no errors are recorded#############
	
	if [[ -e $ERROR_FILE ]]; then
		ERROR_FILE_SIZE=$(stat -c%s "${ERROR_FILE}")
	else
		ERROR_FILE_SIZE=0
	fi
	
	if [[ $ERROR_FILE_SIZE == 0 && -e $ERROR_FILE ]]; then					
		rm $ERROR_FILE ;
	fi
	######sending status mail #############
	send_script_run_mail 2
	
	
	
}

################################################################################################
# Function Name : script_unsuccesful
# Description   : This function updates DWH_C_BATCH_LOG if the script donot complete successfully
################################################################################################	

function script_unsuccesful {

#mail -s "${SCRIPT_NAME}.ksh Failed!!!" ${MAIL_RECEIPIENTS} < ${LOG_FILE}
print_msg "Script Unsuccessful"
    UPDATE_BATCH_LOG_SQL="UPDATE ${TARGET_DB}.DWH_C_BATCH_LOG 
                          SET      
                                END_TIMESTAMP = CURRENT_TIMESTAMP
                                ,STATUS='ERROR'
                                ,BOOKMARK='${BOOKMARK}'
                                ,ERROR_DETAIL='$ret_msg'
								,JOB_ID='${JOB_ID}'
                          WHERE BATCH_ID='${BATCH_ID}'
    					  AND JOB_NAME='${SCRIPT_NAME}'
						  AND START_TIMESTAMP='${GET_TIME_STAMP}'"
                     
                     
    run_query -d "$TARGET_DB" -q "$UPDATE_BATCH_LOG_SQL" -m "Unable to update ${SCRIPT_NAME} log"
	
	exit 1
 }

################################################################################################
# Function Name : aduit_log
# Description   : This function will log the no of rows inserted , updated,rejected or 
#                 deleted when transfering the data from source to destination in  batch operation.
# Parameter     : Level (1= Stage load, 2 = Temp load, 3 = Target load)
################################################################################################	

 function audit_log {
 
    LEVEL=$1
	if [[ -n $2 && -n $3 && -n $4 && -n $5 ]]
	then
		SOURCE=$3
    	DESTINATION=$5
		SOURCE_COUNT_SQL="SELECT COUNT(*) FROM $2.${SOURCE}"
		SOURCE_COUNT=$(get_result -d "${TEMP_DB}" -q "${SOURCE_COUNT_SQL}" -m "Unable to get ${SOURCE} count")
	
    elif [[ $LEVEL -eq 1 ]]
    then
        SOURCE=$(basename $DATA_FILE)
        DESTINATION=$SOURCE_TABLE
		SOURCE_COUNT=$(wc -l < $DATA_FILE)
    elif [[ $LEVEL -eq 2 ]]
    then
        SOURCE=$SOURCE_TABLE
        DESTINATION=$TEMP_TABLE
		SOURCE_COUNT_SQL="SELECT COUNT(*) FROM ${SRC_DB}.${SOURCE}"
		SOURCE_COUNT=$(get_result -d "${SRC_DB}" -q "${SOURCE_COUNT_SQL}" -m "Unable to get ${SOURCE_TABLE} count")
    else
        SOURCE=$TEMP_TABLE
    	DESTINATION=$TARGET_TABLE
		SOURCE_COUNT_SQL="SELECT COUNT(*) FROM ${TEMP_DB}.${SOURCE}"
		SOURCE_COUNT=$(get_result -d "${TEMP_DB}" -q "${SOURCE_COUNT_SQL}" -m "Unable to get ${SOURCE} count")
    fi
	
	
    	 
    print_msg "Source     : $SOURCE"
    print_msg "Destination: $DESTINATION"
	
	print_msg "SQL Statistics:"
	print_msg "NO OF ROWS INSERTED: `if [[ $NO_OF_ROW_INSERTED -eq 0 ]]; then echo "0"; else echo "${NO_OF_ROW_INSERTED}"; fi`"
    print_msg "NO OF ROWS DELETED:  `if [[ $NO_OF_ROW_DELETED -eq 0 ]]; then echo "0"; else echo "${NO_OF_ROW_DELETED}"; fi`"
	print_msg "No OF ROWS UPDATED:  `if [[ $NO_OF_ROW_UPDATED -eq 0 ]]; then echo "0"; else echo "${NO_OF_ROW_UPDATED}"; fi`"
	
    ADUIT_LOG_SQL="MERGE INTO ${TARGET_DB}.DWH_C_AUDIT_LOG AS  A
                   USING (
    					 SELECT ${BATCH_ID} BATCH_ID 
    							,${JOB_ID} JOB_ID
    							,${LEVEL} LEVEL
    					 ) AS TMP
    					 ON A.BATCH_ID = TMP.BATCH_ID
    					  AND A.JOB_ID = TMP.JOB_ID
    					  AND A.LEVEL = TMP.LEVEL
    					 WHEN MATCHED THEN UPDATE
            					 SET NUMBERS_OF_ROW_INSERTED = NUMBERS_OF_ROW_INSERTED+$NO_OF_ROW_INSERTED
    					 		    ,NUMBERS_OF_ROW_UPDATED  = NUMBERS_OF_ROW_UPDATED+$NO_OF_ROW_UPDATED
    					 		    ,NUMBERS_OF_ROW_DELETED  = NUMBERS_OF_ROW_DELETED+$NO_OF_ROW_DELETED
    					 		    ,NUMBERS_OF_ROW_REJECTED = NUMBERS_OF_ROW_REJECTED+${NO_OF_ROW_REJECTED}
									,AUDIT_TIMESTAMP = CURRENT_TIMESTAMP
    					 WHEN NOT MATCHED THEN INSERT
                   			   (	 BATCH_ID                
    									,JOB_ID                  
    									,JOB_NAME                
    									,BUSINESS_DATE 
                                        ,AUDIT_TIMESTAMP										
    									,SOURCE        
    									,SOURCE_COUNT            
    									,TARGET         
    									,NUMBERS_OF_ROW_INSERTED 
    									,NUMBERS_OF_ROW_UPDATED  
    									,NUMBERS_OF_ROW_DELETED  
    									,NUMBERS_OF_ROW_REJECTED
    									,LEVEL)
    						VALUES(  TMP.BATCH_ID     
    								,TMP.JOB_ID
    								,'${SCRIPT_NAME}'
    								,$(DATATYPE_CONV "'$CURR_DAY'" DATE 'YYYY-MM-DD')
									,CURRENT_TIMESTAMP
    								,'${SOURCE}'
    								,$SOURCE_COUNT
    								,'${DESTINATION}'
    								,$NO_OF_ROW_INSERTED
    								,$NO_OF_ROW_UPDATED
    								,$NO_OF_ROW_DELETED
    								,$NO_OF_ROW_REJECTED
    								,TMP.LEVEL)
    							"
    			
       
    			
    run_query -d "$TARGET_DB" -q "$ADUIT_LOG_SQL" -m "Unable to update ${SCRIPT_NAME} log"
	


    set_audit_log_var
 }
 
################################################################################################
# Function Name : get_bookmark
# Description   : This function will get bookmark from the DWH_C_BATCH_LOG table.
#                 In case of no bookmark we set if to 'NONE' for fresh run of batch process
# Parameter     : Script Name and Batch ID   
# Value Returns : Bookmark value, if zero then NONE else Bookmark value from DWH_C_BATCH_LOG
################################################################################################


 function get_bookmark {
    TMP_JOBNAME=$1
	TMP_BATCH_ID=$2
	print_msg "Getting bookmark from DWH_C_BATCH_LOG table"
	if [[ -z ${TMP_BATCH_ID} ]]
	then
		GET_BOOKMARK_SQL="SELECT BOOKMARK FROM
							(SELECT BOOKMARK,ROW_NUMBER() OVER (PARTITION BY JOB_NAME,BATCH_ID ORDER BY START_TIMESTAMP DESC,JOB_ID DESC) RNK FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG
							WHERE  JOB_NAME = '${TMP_JOBNAME}'
							AND STATUS='RESTART') AA
						WHERE RNK=1
						"
						
		GET_BOOKMARK_STATUS_SQL="SELECT 1 FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG
						WHERE  JOB_NAME = '${TMP_JOBNAME}'
								AND STATUS IN ('COMPLETE','ERROR')
						"
						
	else						
		GET_BOOKMARK_SQL="SELECT BOOKMARK FROM
						(SELECT BOOKMARK,ROW_NUMBER() OVER (PARTITION BY JOB_NAME,BATCH_ID ORDER BY START_TIMESTAMP DESC,JOB_ID DESC) RNK FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG
							WHERE  JOB_NAME = '${TMP_JOBNAME}'
							AND BATCH_ID = ${TMP_BATCH_ID}
							AND STATUS='RESTART') AA
						WHERE RNK=1
							"
							
		GET_BOOKMARK_STATUS_SQL="SELECT 1 FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG
						WHERE  JOB_NAME = '${TMP_JOBNAME}'
								AND BATCH_ID = ${TMP_BATCH_ID}
								AND STATUS IN ('COMPLETE','ERROR')
						"					
	fi
	
	BOOKMARK=$(get_result -d "${VIEW_DB}" -q "${GET_BOOKMARK_SQL}" -m "Unable to GET_BOOKMARK")
	
	if [[ -z ${BOOKMARK} ]]
    then
       BOOKMARK="NONE"
	   export BOOKMARK
	else
	   export BOOKMARK
    fi
	print_msg "The bookmark is : ${BOOKMARK}"

	if [[ ${MODULE_TYPE} = 'CYC' ]];then
		GET_BOOKMARK_STATUS_SQL="SELECT 1 FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_BATCH_LOG
						WHERE  JOB_NAME = '${TMP_JOBNAME}'
								AND BATCH_ID = ${TMP_BATCH_ID}
								AND STATUS IN ('RUNNING','ERROR')
						"		
		print_msg "Checking for run-ability of the job for the batch"
		BOOKMARK_STATUS=$(get_result -d "${VIEW_DB}" -q "$GET_BOOKMARK_STATUS_SQL" -m "Unable to GET_BOOKMARK_STATUS")
		
		if [[ -z ${BOOKMARK_STATUS} ]]
		then
			print_msg "Job ready for the run for the batch"
			print_bookmark "RUNNING CODES FOR BOOKMARK = ${BOOKMARK}"
		else
			######sending status mail #############
			print_msg "Previous cycle of ${TMP_JOBNAME}.ksh in ERROR or RUNNING state"
			send_script_run_mail 22
			exit 1
		fi
		
	else
		print_msg "Checking for run-ability of the job for the batch"
		BOOKMARK_STATUS=$(get_result -d "${VIEW_DB}" -q "$GET_BOOKMARK_STATUS_SQL" -m "Unable to GET_BOOKMARK_STATUS")
		
		if [[ -z ${BOOKMARK_STATUS} ]]
		then
			print_msg "Job ready for the run for the batch"
			print_bookmark "RUNNING CODES FOR BOOKMARK = ${BOOKMARK}"
		else
			######sending status mail #############
			print_msg "Tried running the script ${TMP_JOBNAME}.ksh again today"
			send_script_run_mail 22
			print_msg "Exiting due to re-run of the script without restarting the script parameters."
			print_msg "Sql: ${GET_BOOKMARK_STATUS_SQL}"
			exit 1
		fi
	fi
		
}

 
################################################################################################
# Function Name : get_current_dt
# Description   : This function will get current day date  from the DWH_D_CURR_TIME_LU table.
#
# Value Returns : exports CURR_DAY
################################################################################################
 
 function get_current_dt {

	print_msg "Getting Current Date"
   GET_CURRENT_DT="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE  PARAM_NAME='BUSINESS_DATE'"
   
   CURR_DAY=$(get_result -d "${VIEW_DB}" -q "$GET_CURRENT_DT" -m "Unable to get GET_CURRENT_DT")
   print_msg "Current date for the job = ${CURR_DAY}"
   echo ""
   export CURR_DAY
}

################################################################################################
# Function Name : get_load_mode
# Description   : This function gets PARAM_VALUE to determine delta and full load from 
#                 DWH_C_PARAM table.
#
# Value Returns : exports LOAD_MODE 
################################################################################################

 function get_load_mode {
	print_msg "Getting load mode for the job"
   GET_LOAD_MODE="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE PARAM_NAME='Insert Mode'"
   
   LOAD_MODE=$(get_result -d "${VIEW_DB}" -q "$GET_LOAD_MODE" -m "Unable to get GET_LOAD_MODE")
   print_msg "The load mode for the job = ${LOAD_MODE}"
   echo ""
   export LOAD_MODE
}


################################################################################################
# Function Name : get_primary_currency
# Description   : This function gets Primary_currency to determine exchange rate factor
#                 DWH_C_PARAM table.
#
# Value Returns : exports Primary Currency 
################################################################################################

 function get_primary_currency {

	print_msg "Getting primary currency for the job"
	GET_PRM_CUR="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE PARAM_NAME='Primary Currency'"
   
	Primary_Currrency=$(get_result -d "${VIEW_DB}" -q "$GET_PRM_CUR" -m "Unable to get Primary Currency")
	print_msg "The primary currency for the job = ${Primary_Currrency}"
	echo ""
	export Primary_Currrency
}

################################################################################################
# Function Name : get_image_fs_prefix
# Description   : This function gets the fileshare path from DWH_C_PARAM table to append to the 
#                 image file name.
#
# Value Returns : exports image fileshare prefix
################################################################################################

 function get_image_fs_path {

	print_msg "Getting image file share path for the job"
	GET_IMG_PTH="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE PARAM_NAME='IMG_FILE_SHARE_PATH'"
   
	IMG_FILE_SHARE_PATH=$(get_result -d "${VIEW_DB}" -q "$GET_IMG_PTH" -m "Unable to get Primary Currency")
	print_msg "The image fieshare path for the job = ${IMG_FILE_SHARE_PATH}"
	echo ""
	export IMG_FILE_SHARE_PATH
	
	print_msg "Getting image prefix for the job"
	GET_IMG_PRE="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE PARAM_NAME='IMG_PREFIX'"
   
	IMG_PREFIX=$(get_result -d "${VIEW_DB}" -q "$GET_IMG_PRE" -m "Unable to get Primary Currency")
	print_msg "The image prefix for the job = ${IMG_PREFIX}"
	echo ""
	export IMG_PREFIX
	
	print_msg "Getting image suffix for the job"
	GET_IMG_SUF="SELECT PARAM_VALUE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_PARAM WHERE PARAM_NAME='IMG_SUFFIX'"
   
	IMG_SUFFIX=$(get_result -d "${VIEW_DB}" -q "$GET_IMG_SUF" -m "Unable to get Primary Currency")
	print_msg "The image suffix  path for the job = ${IMG_SUFFIX}"
	echo ""
	export IMG_SUFFIX
}

###############################################################################################
# Function Name : set_bookmark
# Description   : This function will set the bookmark value in DWH_C_BATCH_LOG table
# 9/12/2014 
# Parameter     : Bookmark value from the script (eg: set_bookmark "AFTER_STG_LOAD")
################################################################################################

function set_bookmark {
	print_msg "Setting bookmark value for the script in DWH_C_BATCH_LOG table"
	export BOOKMARK="$1"
	SET_BOOKMARK_SQL="UPDATE ${TARGET_DB}.DWH_C_BATCH_LOG
                      SET BOOKMARK='${BOOKMARK}'
					  ,JOB_ID=${JOB_ID}
                      WHERE BATCH_ID=${BATCH_ID}
                       AND JOB_NAME = '${SCRIPT_NAME}'
					   AND START_TIMESTAMP='${GET_TIME_STAMP}'
                     "
    run_query -d "$TARGET_DB" -q "$SET_BOOKMARK_SQL" -m "Unable to update BOOKMARK in DWH_C_BATCHA_LOG ${SCRIPT_NAME} log"					 
	#echo ${BOOKMARK} > ${BOOKMARK_FILE}
	
	
	print_bookmark "RUNNING CODES FOR BOOKMARK = ${BOOKMARK}"
	
}


##################################################################################################
# Function Name : set_audit_log_var
# Description   : This function is used to hold the variables value used for DWH_C_AUDIT_LOG table
# 9/12/2014 
###################################################################################################

function set_audit_log_var {
	NO_OF_ROW_INSERTED=0
    NO_OF_ROW_UPDATED=0
    NO_OF_ROW_DELETED=0
    NO_OF_ROW_REJECTED=0
}

############################################################################################################
# Function Name : set_activity_count
# Description   : This function will is used to store incremented value of count for 
#                 NO_OF_ROW_INSERTED,NO_OF_ROW_UPDATED,NO_OF_ROW_DELETED,NO_OF_ROW_REJECTED in target table
# 9/12/2014 
# Parameter     : Insert or Update or Reject or Delete (eg: set_activity_count 'update')
#############################################################################################################

function set_activity_count {                            
  print_msg "set_activity_count"
    type=$1
    
	case $type in
        insert)
		    ((NO_OF_ROW_INSERTED=NO_OF_ROW_INSERTED+$(get_activity_count)))
			;;
        update)
            ((NO_OF_ROW_UPDATED=NO_OF_ROW_UPDATED+$(get_activity_count)))
            ;;
	    delete)
		    ((NO_OF_ROW_DELETED=NO_OF_ROW_DELETED+$(get_activity_count))) ;;
		reject)
		    ((NO_OF_ROW_REJECTED=NO_OF_ROW_REJECTED+$(get_activity_count))) ;;
		fastload)
			((NO_OF_ROW_INSERTED=NO_OF_ROW_INSERTED+$(get_fastload_insert_count)))
		    ((NO_OF_ROW_REJECTED=NO_OF_ROW_REJECTED+$(get_fastload_err1_count)+$(get_fastload_err2_count))) 
			;;
		mload)
			((NO_OF_ROW_INSERTED=NO_OF_ROW_INSERTED+$(get_mload_insert_count)))
    esac
   print_msg "set_activity_count end"
}
 

############################################################################################################
# Function Name : check_file
# Description   : This function will check if the data file of same name already exist in archive folder
# 11/17/2014 
#############################################################################################################

function check_file {
if [[ -f ${ARCHIVE}/$(basename ${DATA_FILE}) ]]
then
    print_msg "ERROR: File exist in archive. Exiting..."
    exit 1
fi
}

############################################################################################################
# Function Name : add_file_checksum
# Description   : This function will add row count and md5 checksum of file to header and footer of file
# 11/17/2014 
#############################################################################################################

#function add_file_checksum {
#    FILE_NAME="$DATA_FILE"
#	echo $FILE_NAME
#    md5sum ${FILE_NAME} | cut -d" " -f1 > ${DATA_DIR}/header
#    ret_code1=$?
#    count_tmp=$(wc -l < ${FILE_NAME})
#	ret_code2=$?
#    echo ${count_tmp} >> ${DATA_DIR}/header
#    cat ${DATA_DIR}/header ${FILE_NAME} ${DATA_DIR}/header > ${FILE_NAME}_temp
#	ret_code3=$?
#   
#    if [[ $ret_code1 != "0" || $ret_code2 != "0" || $ret_code3 != "0" ]]; then
#        print_err "ERROR"
#	    exit 1
#    fi
#	
#    mv -f ${FILE_NAME}_temp ${FILE_NAME}
#    rm -f  ${FILE_NAME}_temp
#	rm -f ${DATA_DIR}/header
#}

function add_file_checksum {
    FILE_NAME="$DATA_FILE"
	HEADER="$SCRIPT_NAME"'_header'
	echo $FILE_NAME
    md5sum ${FILE_NAME} | cut -d" " -f1 > ${DATA_DIR}/${HEADER}
	ret_code1=$?
	COUNT_TEMP=$(wc -l < ${FILE_NAME})
	ret_code2=$?
	echo ${COUNT_TEMP} >> ${DATA_DIR}/${HEADER}
    cat ${DATA_DIR}/${HEADER} ${FILE_NAME} ${DATA_DIR}/${HEADER} > ${FILE_NAME}_temp
	ret_code3=$?
    
    if [[ $ret_code1 != "0" || $ret_code2 != "0" || $ret_code3 != "0" ]]; then
        print_err "ERROR"
	    exit 1
    fi
	
    mv -f ${FILE_NAME}_temp ${FILE_NAME}
    rm -f ${FILE_NAME}_temp
	rm -f ${DATA_DIR}/${HEADER}
}

############################################################################################################
# Function Name : check_file_integrity
# Description   : This function will truncate the header and footer of file and calculate the md5 value of
#                 truncated file and make sure the integrity of the file is intact.
# 11/19/2014 
############################################################################################################
function check_file_integrity {

	print_msg "Checking integrity of the file $FILE_NAME"
    FILE_NAME=${DATA_FILE}
	echo "FILE_NAME="$FILE_NAME
	if [[ -f ${DATA_FILE} ]]
	then
	    md5_value_top=$(head -1 ${FILE_NAME})
        count_top=$(head -2 ${FILE_NAME} | tail -1)
        
	    print_msg "md5value: $md5_value_top"
        print_msg "No of rows :$count_top"	
		
	    if [[ $count_top != +([0-9]) ]]
		then
			echo "File integrity checksum not present."
			chk_err -r 1 -m "File integrity checksum not present"
			
			exit 1
		fi
		
	    (( md5_line = ${count_top} + 3 ))
	    #echo "md5_line :${md5_line}"
	    md5_value_down=$(tail -2 ${FILE_NAME} | head -1)
        count_down=$(tail -1 ${FILE_NAME})
		
		if [[ $count_down != +([0-9]) ]]
		then
			echo "File integrity checksum not present."
			chk_err -r 1 -m "File integrity checksum not present"
			
			exit 1
		fi
		
		
	    (( count = ${count_top} + 2 ))	
		
	    head -${count} ${FILE_NAME}|tail -n +3 > ${FILE_NAME}_temp 
		echo "mv ${FILE_NAME}_temp ${FILE_NAME}"
        mv -f ${FILE_NAME}_temp ${FILE_NAME}
		echo "rm -f ${FILE_NAME}_temp"
        rm -f ${FILE_NAME}_temp  
		
		
		print_msg "md5value: $md5_value_down"
        print_msg "No of rows :$count_down"
		
	    md5_value=$(md5sum ${FILE_NAME} | cut -d" " -f1)
	    count_new=$(wc -l < ${FILE_NAME})
	    print_msg "${md5_value} and count ${count_new}"
		
	    if [[ ${md5_value} != ${md5_value_top} || ${md5_value} != ${md5_value_down} || ${count_new} -ne ${count_down} || ${count_new} -ne ${count_top} ]]
	    then
	    	print_msg "Data integrity lost !!"
			chk_err -r 1 -m "Data integrity lost"
			
	        exit 1
	    fi
	else
	    print_msg "${DATA_FILE} not found, Exiting..."
		chk_err -r 1 -m "${DATA_FILE} not found"
		
		exit 1
	fi

}

############################################################################################################
# Function Name : ods_cyclic_start
# Description   : This function will start the cyclic job with differnt check before start. 
# 02/17/2017 
############################################################################################################
function ods_cyclic_start {

	print_msg "Get last run status "
	LAST_RUN_BATCH_STATUS_SQL="SELECT coalesce(MAX(CASE WHEN STATUS = 'COMPLETE' THEN 0  WHEN STATUS = 'RESTART' THEN 1 ELSE 2 END),0)  FROM ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE} WHERE BATCH_ID = ${LAST_RUN_BATCH_ID} and TABLE_NAME = '${TARGET_TABLE}'"
	
	LAST_RUN_BATCH_STATUS=$(get_result -d "$TARGET_DB" -q "$LAST_RUN_BATCH_STATUS_SQL" -m "Unable get run status")
	LAST_RUN_BATCH_STATUS=$(("$LAST_RUN_BATCH_STATUS"))
	
	echo "Last Run Status is: $LAST_RUN_BATCH_STATUS"
	
	export BOOKMARK="NONE"
	
	if [[ $LAST_RUN_BATCH_STATUS == 1 ]] 
	then
		#export CURRENT_LOAD_BATCH_ID=$LAST_RUN_BATCH_ID
		
		UPDATE_BATCH_LOG_SQL="UPDATE ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE}  
                     SET      
                           STATUS='RESTARTED'
                     WHERE BATCH_ID='${LAST_RUN_BATCH_ID}'
							AND TABLE_NAME='${TARGET_TABLE}'"                    
    
		run_query -d "$TARGET_DB" -q "$UPDATE_BATCH_LOG_SQL" -m "Unable to update ${SCRIPT_NAME} log"
	
		print_msg "Get Bookmark"
		BOOKMARK_SQL="SELECT coalesce(BOOKMARK,'NONE')  FROM ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE} WHERE BATCH_ID = ${LAST_RUN_BATCH_ID} and TABLE_NAME = '${TARGET_TABLE}'"
	
		export BOOKMARK=$(get_result -d "$TARGET_DB" -q "$BOOKMARK_SQL" -m "Unable get run Bookmark")
	fi
	
	if [[ $LAST_RUN_BATCH_STATUS == 2 ]] 
	then
		chk_err -r 1 -m "Last Batch Not-Completed/Error Out"
	fi
	
	
	print_msg "Checking the data in Stage"
	STAGE_ROW_COUNT_SQL="SELECT coalesce(COUNT(*),0)  FROM ${SRC_DB}.${STAGE_TABLE}"
	
	run_query -d "$SRC_DB" -q "$STAGE_ROW_COUNT_SQL" -m "Unable to Run the Stage Select" 
	STAGE_ROW_COUNT=$(get_result -d "$SRC_DB" -q "$STAGE_ROW_COUNT_SQL" -m "Unable get row counts")
	
	echo "Stage Record count is: $STAGE_ROW_COUNT"
	
	if [[ $STAGE_ROW_COUNT == 0 ]] 
	then
		print_msg "No new records to process"
		exit 0
	fi	

	print_msg "Inserting batch log"
	INSERT_BATCH_LOG_SQL="INSERT INTO ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE} (
                        TABLE_NAME 
    				   ,BATCH_ID
    				   ,STATUS
    				   ,START_TS
    				   ,END_TS
					   ,BOOKMARK)
                   SELECT '${TARGET_TABLE}'
                         ,${CURRENT_LOAD_BATCH_ID}
                         ,'RUNNING'
                        ,CURRENT_TIMESTAMP
    					 ,NULL
						 ,'${BOOKMARK}'
                   "
    
    run_query -d "$TARGET_DB" -q "$INSERT_BATCH_LOG_SQL" -m "Unable to start ${SCRIPT_NAME} log"   
}


############################################################################################################
# Function Name : ods_cyclic_end
# Description   : This function will end the cyclic job as update the batch log as 'COMPLETE'. 
# 02/17/2017 
############################################################################################################

function ods_cyclic_end {
	print_msg "Script Successful"
    UPDATE_BATCH_LOG_SQL="UPDATE ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE}  
                     SET      
                            END_TS = CURRENT_TIMESTAMP
                            ,STATUS='COMPLETE'
							,BOOKMARK='COMPLETE'
                     WHERE BATCH_ID='${CURRENT_LOAD_BATCH_ID}'
							AND TABLE_NAME='${TARGET_TABLE}'"                    
    
    run_query -d "$TARGET_DB" -q "$UPDATE_BATCH_LOG_SQL" -m "Unable to update ${SCRIPT_NAME} log"
	
	if [[ -e $ERROR_FILE ]]; then
		ERROR_FILE_SIZE=$(stat -c%s "${ERROR_FILE}")
	else
		ERROR_FILE_SIZE=0
	fi
	
	if [[ $ERROR_FILE_SIZE == 0 && -e $ERROR_FILE ]]; then					
		rm $ERROR_FILE ;
	fi
	
}

############################################################################################################
# Function Name : ods_cyclic_error
# Description   : This function will be called whenever there is error in the ods cyclic sciprt 
#					, this funcation will update the batch log table to mark it as 'ERROR' and will end the job. 
# 02/17/2017 
############################################################################################################

function ods_cyclic_error {

print_msg "Script Unsuccessful"
    UPDATE_BATCH_LOG_SQL="UPDATE ${TARGET_DB}.${LOAD_BATCH_LOG_TABLE} 
                          SET      
                                END_TS = CURRENT_TIMESTAMP
                                ,STATUS='ERROR'
                                ,BOOKMARK='${BOOKMARK}'
                          WHERE BATCH_ID='${CURRENT_LOAD_BATCH_ID}'
    					  AND TABLE_NAME='${TARGET_TABLE}'"
                     
                     
    run_query -d "$TARGET_DB" -q "$UPDATE_BATCH_LOG_SQL" -m "Unable to update Batch log"
	
	exit 1
 }

 