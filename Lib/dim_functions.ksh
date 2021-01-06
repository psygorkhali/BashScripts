######################################################################################
# script : fact_functions.ksh                                                    
# Description : This library function holds the fact processing functions        
# Modifications                                                                  
#9/12/2014   : Logic  : Initial Script                                                           
######################################################################################

######################################################################################
# Function Name : insert_from_temp
# Description   : This function loads target table, selecting data from temp table
######################################################################################

function insert_from_temp {
	print_msg ""
	print_msg "###################################################################################"
   print_msg "Inserting new records into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
   if [[ -n $DIM_IDNT ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
								(  ${DIM_KEY}
								   , ${DIM_IDNT_LIST}
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , RCD_INS_TS
								   , RCD_UPD_TS
								   ${CLOSE_MAINTAINANCE_COLUMNS}
							   )
								SELECT COALESCE((SELECT MAX($DIM_KEY) FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} WHERE $DIM_KEY>=0), 0) + RANK() OVER (ORDER BY ${DIM_IDNT_LIST})
								   , ${DIM_IDNT_LIST}
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								   ${SET_CLOSE_MAINTAINANCE_COLUMNS}
								FROM ${TEMP_DB}.${TEMP_TABLE}  src
								WHERE NOT EXISTS
								 (SELECT 1 
									FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} tgt WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK})
								  "
      
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Keys required to insert records using temporary table"
   fi
   
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
   print_msg "###################################################################################"
   print_msg ""
   set_activity_count insert
   audit_log 3
}

######################################################################################
# Function Name : insert_from_temp_cyclic
# Description   : This function loads cyclic target table, selecting data from temp table
######################################################################################

function insert_from_temp_cyclic {
	print_msg ""
	print_msg "###################################################################################"
   print_msg "Inserting new records into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
   if [[ -n $DIM_IDNT ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
								(  ${DIM_KEY}
								   , ${DIM_IDNT_LIST}
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , RCD_INS_TS
								   , RCD_UPD_TS
								   ${CLOSE_MAINTAINANCE_COLUMNS}
							   )
								SELECT COALESCE((SELECT MAX($DIM_KEY) FROM ${TARGET_DB}.${TARGET_TABLE} WHERE $DIM_KEY>=0), 0) + RANK() OVER (ORDER BY ${DIM_IDNT_LIST})
								   , ${DIM_IDNT_LIST}
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								   ${SET_CLOSE_MAINTAINANCE_COLUMNS}
								FROM (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp)  src
								WHERE NOT EXISTS
								 (SELECT 1 
									FROM ${TARGET_DB}.${TARGET_TABLE} tgt WHERE ${DIM_JOIN_LIST} )
								  AND RNK =1"
      
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Keys required to insert records using temporary table"
   fi
   
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
   print_msg "###################################################################################"
   print_msg ""
   set_activity_count insert
   audit_log 3
}

#########################################################################################
# Function Name : close_using_temp
# Description   : This function closes the dimension records which are no longer active
#				  in the source system. The record close flag is updated from 0 to 1
#########################################################################################

function close_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Closing deleted/stopped records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	then
		  CLOSE_DIMENSION_SQL="UPDATE ${TARGET_DB}.$TARGET_TABLE tgt
							   SET RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								  ,RCD_CLOSE_FLG=1
								  ,RCD_CLOSE_DT=$(DATATYPE_CONV "'$DATEKEY'" DATE 'YYYYMMDD')
							   WHERE NOT EXISTS
								  (SELECT 1 FROM ${TEMP_DB}.$TEMP_TABLE src where ${DIM_JOIN_LIST} and tgt.RCD_CLOSE_FLG=0) 
									 AND $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') BETWEEN tgt.RCD_INS_TS AND tgt.RCD_CLOSE_DT"
									 
	   run_query -d "$TARGET_DB" -q "$CLOSE_DIMENSION_SQL" -m "Unable to Close Dimension Records"
	else
	   chk_err -r 1 -m "Keys required to close records using temporary table"
	fi
	print_msg "Closing deleted/stopped records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}
#########################################################################################
# Function Name : close_using_temp_cyclic
# Description   : This function closes the dimension records of cyclic tables which are no longer active
#				  in the source system. The record close flag is updated from 0 to 1
#########################################################################################

function close_using_temp_cyclic {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Closing deleted/stopped records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	then
		  CLOSE_DIMENSION_SQL="UPDATE tgt
								FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${TEMP_DB}.$TEMP_TABLE src
								  SET RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								  ,RCD_CLOSE_FLG=1
								  ,RCD_CLOSE_DT=$(DATATYPE_CONV "'$DATEKEY'" DATE 'YYYYMMDD')
							   WHERE 
							   ${DIM_JOIN_LIST}
							   AND tgt.RCD_CLOSE_FLG = 0 
							   AND src.SRC_DELETE_FLG = 1
							   AND $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') BETWEEN tgt.RCD_INS_TS AND tgt.RCD_CLOSE_DT"
									 
	   run_query -d "$TARGET_DB" -q "$CLOSE_DIMENSION_SQL" -m "Unable to Close Dimension Records"
	else
	   chk_err -r 1 -m "Keys required to close records using temporary table"
	fi
	print_msg "Closing deleted/stopped records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}


#########################################################################################
# Function Name : close_del_delta
# Description   : This function closes the dimension records which are no longer active
#				  in the source system. The record close flag is updated from 0 to 1
#########################################################################################

function close_del_delta {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Closing deleted/stopped records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	   then
			CLOSE_DIMENSION_SQL="UPDATE ${TARGET_DB}.$TARGET_TABLE tgt
							   SET RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								  ,RCD_CLOSE_FLG=1
								  ,RCD_CLOSE_DT=$(DATATYPE_CONV "'$DATEKEY'" DATE 'YYYYMMDD')
							   WHERE EXISTS
								  (SELECT 1 FROM ${SRC_DB}.${SOURCE_DELETE_TABLE} src where ${DIM_JOIN_LIST})"
									 
			run_query -d "$TARGET_DB" -q "$CLOSE_DIMENSION_SQL" -m "Unable to Close Dimension Records"
	   else
			chk_err -r 1 -m "Keys required to close records using temporary table"
	   fi
	print_msg "Closing deleted/stopped records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}

#########################################################################################
# Function Name : reopen_using_temp
# Description   : This function reopen the dimension records which have been re-activated
#				  in the source system. The record close flag is updated from 1 to 0				  
#########################################################################################

function reopen_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Reopening records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	then
		  REOPEN_DIMENSION_SQL="UPDATE tgt
								FROM ${TARGET_DB}.$TARGET_TABLE tgt, (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp) src
								SET ${UPDATE_COLUMNS_LIST}
									, RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
									, RCD_CLOSE_FLG=0
									, RCD_CLOSE_DT=$(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD')
								WHERE ${DIM_JOIN_LIST} and tgt.RCD_CLOSE_FLG=1 AND RNK=1"
									 
	   run_query -d "$TARGET_DB" -q "$REOPEN_DIMENSION_SQL" -m "Unable to reopen Dimension Records"
	else
	   chk_err -r 1 -m "Keys required to reopen records using temporary table"
	fi
	print_msg "Reopening records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}
#########################################################################################
# Function Name : reopen_using_temp_cyclic
# Description   : This function reopens the dimension records of cyclic tables which have been re-activated
#				  in the source system. The record close flag is updated from 1 to 0				  
#########################################################################################

function reopen_using_temp_cyclic {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Reopening records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	then
		  REOPEN_DIMENSION_SQL="UPDATE tgt
								FROM ${TARGET_DB}.$TARGET_TABLE tgt, (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp) src
								SET ${UPDATE_COLUMNS_LIST}
									, RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
									, RCD_CLOSE_FLG=0
									, RCD_CLOSE_DT=$(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD')
								WHERE ${DIM_JOIN_LIST} 
								AND tgt.RCD_CLOSE_FLG=1 
								AND src.SRC_DELETE_FLG = 0
								AND RNK=1"
									 
	   run_query -d "$TARGET_DB" -q "$REOPEN_DIMENSION_SQL" -m "Unable to reopen Dimension Records"
	else
	   chk_err -r 1 -m "Keys required to reopen records using temporary table"
	fi
	print_msg "Reopening records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}

###########################################################################################
# Function Name : update_using_temp
# Description   : This function updates the target table looking after the temporary table
###########################################################################################

function update_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
										FROM ${TARGET_DB}.$TARGET_TABLE tgt, (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp) src
								   SET ${UPDATE_COLUMNS_LIST}
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
										${UPDATE_CLOSE_MAINTAINANCE_COLUMNS}
									WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK}
									AND (${UPDATE_COLUMN_COMPARE_LIST})
									AND RNK=1"


			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3   
}
###########################################################################################
# Function Name : update_using_temp_cyclic
# Description   : This function updates the cyclic target table looking after the temporary table
###########################################################################################

function update_using_temp_cyclic {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
										FROM ${TARGET_DB}.$TARGET_TABLE tgt, (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp) src
								   SET ${UPDATE_COLUMNS_LIST}
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
										${UPDATE_CLOSE_MAINTAINANCE_COLUMNS}
									WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK}
									AND (${UPDATE_COLUMN_COMPARE_LIST})
									AND src.SRC_DELETE_FLG = 0
									AND RNK=1"


			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3   
}

####################################################################################################
# Function Name : update_closed_using_rollup
# Description   : This function updates the dimension record of target table which record flag is 1
####################################################################################################

function update_closed_using_rollup {

	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating closed records in $TARGET_TABLE"

	   if [[ -n $ROLLUP_TABLE && -n $ROLLUP_KEY ]]
	   then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
									   FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${VIEW_DB}.${VIEW_PREFIX}${ROLLUP_TABLE} src
								   SET ${ROLLUP_UPDATE_COLUMNS_LIST}
							, RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
						 WHERE tgt.${ROLLUP_KEY} = src.${ROLLUP_KEY}
							and tgt.RCD_CLOSE_FLG=1"


			 run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update closed Dimension Records"
	   else
			 chk_err -r 1 -m "Rollup table and Rollup key required to update closed records"
	   fi
	print_msg "Updating closed records in $TARGET_TABLE completed successfully"   
	print_msg "###################################################################################"
	print_msg ""
	
	set_activity_count update
	audit_log 3   
}

####################################################################################################
# Function Name : insert_nokeydimension_from_temp
# Description   : This function loads the dimension data which KEY is not available in target table
####################################################################################################

function insert_nokeydimension_from_temp {
   print_msg ""
   print_msg "###################################################################################"
   print_msg "Inserting new records into $TARGET_TABLE"
   if [[ -n $DIM_IDNT ]]
   then
      INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
                       (  ${DIM_IDNT_LIST}
                       $(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
                       , RCD_INS_TS
                       , RCD_UPD_TS
                       ${CLOSE_MAINTAINANCE_COLUMNS}
                       )
					SELECT ${DIM_IDNT_LIST}
						$(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
					   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
					   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
					   ${SET_CLOSE_MAINTAINANCE_COLUMNS}
					FROM (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_IDNT_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp)  src		
					WHERE NOT EXISTS
							(SELECT 1 FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} tgt WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK})
							AND RNK =1"
      
   run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Identifier required to insert records using temporary table"
   fi
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
   print_msg ""
   
   set_activity_count insert
   audit_log 3
}

####################################################################################################
# Function Name : delete_nokeydimension_from_temp
# Description   : This function deletes the dimension data which KEY is available in target table
####################################################################################################

function delete_nokeydimension_from_temp {
	print_msg ""
	print_msg "###################################################################################"
    print_msg "Deleting deleted/stopped records in $TARGET_TABLE"
    if [[ -n $DIM_IDNT ]]
    then
          DELETE_SQL="DELETE FROM ${TARGET_DB}.${TARGET_TABLE} 
						WHERE (${DIM_IDNT_LIST}) IN (SELECT ${DIM_IDNT_LIST} FROM ${TEMP_DB}.${TEMP_TABLE})"
                                     
          run_query -d "$TARGET_DB" -q "$DELETE_SQL" -m "Unable to Delete Records from the $TARGET_TABLE"
    else
          chk_err -r 1 -m "Identifier required to delete records using temporary table"
    fi
    print_msg "Records Deleted from ${TARGET_DB}.${TARGET_TABLE} Sucessfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count delete
   audit_log 3
}

####################################################################################################
# Function Name : mtx_insert_from_temp
# Description   : This function loads the dimension data which KEY is not available in target table
####################################################################################################

function mtx_insert_from_temp {
	print_msg ""
	print_msg "###################################################################################"
   print_msg "Inserting new records into $TARGET_TABLE"
   if [[ -n $DIM_KEY ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
                       (  ${DIM_KEY_LIST}
                       $(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
                       , RCD_INS_TS
                       , RCD_UPD_TS
                       ${CLOSE_MAINTAINANCE_COLUMNS}
                       )
						SELECT ${DIM_KEY_LIST}
						   $(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
						   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
						   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
						   ${SET_CLOSE_MAINTAINANCE_COLUMNS}
					  FROM (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_KEY_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp)  src
						WHERE NOT EXISTS
							(SELECT 1 FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} tgt WHERE ${DIM_KEY_JOIN_LIST} ${DIMENSION_OPEN_CHECK})
							AND RNK =1"
      
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
		chk_err -r 1 -m "Identifier required to insert records using temporary table"
   fi
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
   print_msg ""
   
   set_activity_count insert
   audit_log 3
}
####################################################################################################
# Function Name : mtx_insert_from_temp_cyclic
# Description   : This function loads the dimension data which KEY is not available in target table
####################################################################################################

function mtx_insert_from_temp_cyclic {
	print_msg ""
	print_msg "###################################################################################"
   print_msg "Inserting new records into $TARGET_TABLE"
   if [[ -n $DIM_KEY ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
                       (  ${DIM_KEY_LIST}
                       $(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
                       , RCD_INS_TS
                       , RCD_UPD_TS
                       ${CLOSE_MAINTAINANCE_COLUMNS}
                       )
						SELECT ${DIM_KEY_LIST}
						   $(if [[ -n $TEMP_TABLE_COLUMN ]]; then echo ", ${TEMP_TABLE_COLUMN_LIST}"; fi)
						   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
						   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
						   ${SET_CLOSE_MAINTAINANCE_COLUMNS}
					  FROM (SELECT tmp.*, ROW_NUMBER() OVER (PARTITION BY ${DIM_KEY_LIST} ORDER BY CURRENT_DATE) RNK FROM ${TEMP_DB}.${TEMP_TABLE} tmp)  src
						WHERE NOT EXISTS
							(SELECT 1 FROM ${TARGET_DB}.${TARGET_TABLE} tgt WHERE ${DIM_KEY_JOIN_LIST} ${DIMENSION_OPEN_CHECK})
							AND RNK =1"
      
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
		chk_err -r 1 -m "Identifier required to insert records using temporary table"
   fi
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
   print_msg ""
}

####################################################################################################
# Function Name : mtx_delete_notin_temp
# Description   : This function loads the dimension data which KEY is not available in target table
####################################################################################################

function mtx_delete_notin_temp {
	print_msg ""
	print_msg "###################################################################################"
    print_msg "Deleting deleted/stopped records in $TARGET_TABLE"
    if [[ -n $DIM_KEY ]]
    then
          DELETE_SQL="DELETE FROM ${TARGET_DB}.${TARGET_TABLE} 
						WHERE (${DIM_KEY_LIST}) NOT IN (SELECT ${DIM_KEY_LIST} FROM ${TEMP_DB}.${TEMP_TABLE})"
                                     
          run_query -d "$TARGET_DB" -q "$DELETE_SQL" -m "Unable to Delete Records from the $TARGET_TABLE"
    else
          chk_err -r 1 -m "Identifier required to delete records using temporary table"
    fi
    print_msg "Records Deleted from ${TARGET_DB}.${TARGET_TABLE} Sucessfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count delete
   audit_log 3
}
####################################################################################################
# Function Name : mtx_delete_in_temp_and_tgt_cyclic
# Description   : This function deletes the dimension data whose KEY is available in target table and has delete flag = 1 in source.
####################################################################################################

function mtx_delete_in_temp_and_tgt_cyclic {
	print_msg ""
	print_msg "###################################################################################"
    print_msg "Deleting deleted/stopped records in $TARGET_TABLE"
    if [[ -n $DIM_KEY ]]
    then
          DELETE_SQL="DELETE FROM ${TARGET_DB}.${TARGET_TABLE} 
						WHERE (${DIM_KEY_LIST}) IN (SELECT ${DIM_KEY_LIST} FROM ${TEMP_DB}.${TEMP_TABLE}
						WHERE SRC_DELETE_FLG = 1)"
                                     
          run_query -d "$TARGET_DB" -q "$DELETE_SQL" -m "Unable to Delete Records from the $TARGET_TABLE"
    else
          chk_err -r 1 -m "Identifier required to delete records using temporary table"
    fi
    print_msg "Records Deleted from ${TARGET_DB}.${TARGET_TABLE} Sucessfully"
	print_msg "###################################################################################"
	print_msg ""


}

######################################################################################################################
# Function Name : set_dimension_variable
# Description   : This functions initializes and sets various dimension columns like maintenance namely  
#                 RCD_CLOSE_FLG and RCD_CLOSE_DT. It also creates join conditions for various operations.
#                 This function generates update list from temp table which is the list of all columns 
#                 which are to be which are to be updated and then it also generates update clause to 
#                 fed into update column.  Also rollup fields are generated and respective update clause is generated  
######################################################################################################################

function set_dimension_variable
{
   
   CLOSE_MAINTAINANCE_COLUMNS=""
   SET_CLOSE_MAINTAINANCE_COLUMNS=""
   UPDATE_CLOSE_MAINTAINANCE_COLUMNS=""
   DIMENSION_OPEN_CHECK=""
   DIM_KEY_LIST=""
   DIM_KEY_JOIN_LIST=""
   DIM_JOIN_LIST=""
   DIM_IDNT_LIST=""
   DIM_COL_LIST=""
   UPDATE_COLUMNS_LIST=""
   UPDATE_COLUMNS_LIST=""
   UPDATE_COLUMN_COMPARE_LIST=""
   
   if [[ "${DIM_CLOSABLE}" == "1" ]]; then
      export CLOSE_MAINTAINANCE_COLUMNS=", RCD_CLOSE_FLG
                       , RCD_CLOSE_DT"

      export SET_CLOSE_MAINTAINANCE_COLUMNS=", 0 RCD_CLOSE_FLG
                   , $(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD') RCD_CLOSE_DT"
                   
      export UPDATE_CLOSE_MAINTAINANCE_COLUMNS=", RCD_CLOSE_FLG=0
                        , RCD_CLOSE_DT=$(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD')"
   
      export DIMENSION_OPEN_CHECK="and tgt.RCD_CLOSE_FLG=0"
   fi

    CHECK=1
   
    for field in $DIM_KEY
   do
   
      if [[ $CHECK -eq 1 ]]
      then
         DIM_KEY_LIST="$field"
         DIM_KEY_JOIN_LIST="src.$field=tgt.$field"
         CHECK=0
      else
         DIM_KEY_LIST="${DIM_KEY_LIST}, $field"
         DIM_KEY_JOIN_LIST="${DIM_KEY_JOIN_LIST} AND src.$field=tgt.$field"
      fi
      
   done
   
   
   CHECK=1
   
    for field in $DIM_IDNT
   do
   
      if [[ $CHECK -eq 1 ]]
      then
         DIM_JOIN_LIST="src.$field=tgt.$field"
         DIM_IDNT_LIST="$field"
		 DIM_IDNT_SRC_LIST="src.$field"
         CHECK=0
      else
         DIM_JOIN_LIST="${DIM_JOIN_LIST} AND src.$field=tgt.$field"
         DIM_IDNT_LIST="${DIM_IDNT_LIST}, $field"
		 DIM_IDNT_SRC_LIST="${DIM_IDNT_SRC_LIST}, src.$field"
      fi
      
   done
   
   CHECK=1
   
    for field in $DIM_COL
   do
   
      if [[ $CHECK -eq 1 ]]
      then
         DIM_COL_LIST="$field"
         CHECK=0
      else
         DIM_COL_LIST="${DIM_COL_LIST}, $field"
      fi
      
   done
   

    CHECK=1
   for update_column in $TEMP_TABLE_COLUMN
   # at the end of above for loop, the script will have two new variables:
   # $UPDATE_COLUMNS_LIST => holds columns to be updated
   # $UPDATE_COLUMN_COMPARE_LIST => holds where clause to be fed in the update statement

   do
      # at first, all operations inside this if clause have to be done. So, the variable CHECK was initialized
      if [[ $CHECK -eq 1 ]] #condition is fulfilled
         # Update column list is src variable that will hold the string containing all the columns that are to be updated
         # here, the very first updatable column is being initialized
         then UPDATE_COLUMNS_LIST="$update_column=src.$update_column"
         
         TEMP_TABLE_COLUMN_LIST="$update_column"
		 
		 ######################################################################################
         ### Begin 3/25/2019: Logic modified for UPDATE_COLUMN_COMPARE_LIST.                ###
		 ### Desc: Defaulting to 0 may not always capture change if 0 is a permitted value. ###
		 ######################################################################################
         ## checks if the column is any of the date or timestamp columns
         #if [[ $update_column == *_DT || $update_column == *_TS ]] 
         #   # this initialization facilitates where clause in update statement for maintenance columns. The maintenance columns
         #   # have to be checked, whether the dates are current or past
         #   then UPDATE_COLUMN_COMPARE_LIST="COALESCE(src.$update_column, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$update_column, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS'))"
         #else
         #   # Columns other than maintenance columns
         #   UPDATE_COLUMN_COMPARE_LIST="COALESCE(src.$update_column,'0') <> COALESCE(tgt.$update_column,'0')"			
         #fi
		 
		 UPDATE_COLUMN_COMPARE_LIST="src.$update_column <> tgt.$update_column OR (src.$update_column IS NULL AND tgt.$update_column IS NOT NULL) OR (src.$update_column IS NOT NULL AND tgt.$update_column IS NULL)"
		 
		 ######################################################################################
		 ### End 3/25/2019: Logic modified for UPDATE_COLUMN_COMPARE_LIST.                  ###
		 ######################################################################################
      CHECK=0 # assign zero to check variable. This helps further, as the string containing updatable columns and where clause
            # have now to be concatenated, to accomodate all the fields separated by src ','
      else
         # concatenate next column
         UPDATE_COLUMNS_LIST="$UPDATE_COLUMNS_LIST, $update_column=src.$update_column"
         
         TEMP_TABLE_COLUMN_LIST="${TEMP_TABLE_COLUMN_LIST}, $update_column"
         
		 ######################################################################################
         ### Begin 3/25/2019: Logic modified for UPDATE_COLUMN_COMPARE_LIST.                ###
		 ### Desc: Defaulting to 0 may not always capture change if 0 is a permitted value. ###
		 ######################################################################################		 
         #if [[ $update_column == *_DT || $update_column == *_TS ]]
         #   # concatenate next column
         #   then UPDATE_COLUMN_COMPARE_LIST="$UPDATE_COLUMN_COMPARE_LIST OR COALESCE(src.$update_column, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$update_column, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS'))"
         #else
         #   # concatenate next column
         #   UPDATE_COLUMN_COMPARE_LIST="$UPDATE_COLUMN_COMPARE_LIST OR COALESCE(src.$update_column,'0') <> COALESCE(tgt.$update_column,'0')"
         #      
         #fi
		 
		 if [[ $update_column == "ASSOC_CUS_ID" ]]
		    then UPDATE_COLUMN_COMPARE_LIST="$UPDATE_COLUMN_COMPARE_LIST OR HASHROW(src.$update_column) <> HASHROW(tgt.$update_column)"
		 else
		    UPDATE_COLUMN_COMPARE_LIST="$UPDATE_COLUMN_COMPARE_LIST OR src.$update_column <> tgt.$update_column OR (src.$update_column IS NULL AND tgt.$update_column IS NOT NULL) OR (src.$update_column IS NOT NULL AND tgt.$update_column IS NULL)"
		 fi 
		 
		 ######################################################################################
		 ### End 3/25/2019: Logic modified for UPDATE_COLUMN_COMPARE_LIST.                  ###
		 ######################################################################################		 
      fi
   done
   
   CHECK=1
   
    for field in $ROLLUP_FIELDS
   do
   
      if [[ $CHECK -eq 1 ]]
      then
         ROLLUP_UPDATE_COLUMNS_LIST="$field=src.$field"
         CHECK=0
      else
         ROLLUP_UPDATE_COLUMNS_LIST="${ROLLUP_UPDATE_COLUMNS_LIST}, $field=src.$field"
      fi
      
   done

   
   #IFS="$OIFS"
   return
}

###########################################################################################
# Function Name : scd_close_and_insert_using_temp
# Description   : This function close the old records and insert new records into the target table 
#                 looking after the temporary table.
###########################################################################################

function scd_close_and_insert_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
									FROM ${TARGET_DB}.$TARGET_TABLE TGT,
										 ${TEMP_DB}.${TEMP_TABLE}  SRC
									SET  EFF_TO_DT = SRC.EFF_FROM_DT - 1
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
										,RCD_CLOSE_FLG = 1
										,RCD_CLOSE_DT = $(DATATYPE_CONV "'$CURR_DAY'" DATE 'YYYY-MM-DD')
								    WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK} 
										AND (SRC.${DIM_IDNT_LIST},SRC.EFF_FROM_DT) IN (SELECT SRC.${DIM_IDNT_LIST},MIN(SRC.EFF_FROM_DT) FROM ${TEMP_DB}.${TEMP_TABLE} SRC
											INNER JOIN ${VIEW_DB}.${VIEW_PREFIX}$TARGET_TABLE TGT ON ${DIM_JOIN_LIST}
											WHERE (${UPDATE_COLUMN_COMPARE_LIST}) ${DIMENSION_OPEN_CHECK} 
												AND SRC.EFF_FROM_DT > TGT.EFF_FROM_DT GROUP BY SRC.${DIM_IDNT_LIST} )"
									
		    

			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
					
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
	
	TEMP_TABLE_COLUMN_LIST=$(echo $TEMP_TABLE_COLUMN_LIST | sed 's/, ASSOC_CUS_ID//g')
	
	print_msg ""
	print_msg "###################################################################################"
    print_msg "Inserting new records into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
   if [[ -n $DIM_IDNT ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
								(    ${DIM_KEY}
								   , ${DIM_IDNT_LIST}
								   , EFF_FROM_DT
								   , EFF_TO_DT
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , RCD_INS_TS
								   , RCD_UPD_TS
								   , RCD_TMP_CLOSE_FLG
								   , RCD_CLOSE_FLG
								   , RCD_CLOSE_DT
							   )
							   
							   SELECT COALESCE((SELECT MAX(${DIM_KEY}) FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} WHERE ${DIM_KEY}>=0) , 0) + RANK() OVER ( ORDER BY ${DIM_IDNT_LIST},EFF_FROM_DT) ${DIM_KEY}
								   , ${DIM_IDNT_LIST}
								   , EFF_FROM_DT
								   , EFF_TO_DT
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
								   , RCD_CLOSE_FLG
								   , RCD_CLOSE_FLG
								   ,$(DATATYPE_CONV "'$CURR_DAY'" DATE 'YYYY-MM-DD')
							   
								FROM ${TEMP_DB}.${TEMP_TABLE}  SRC
								 WHERE NOT EXISTS
								  (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST} 
										FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} 
										WHERE RCD_TMP_CLOSE_FLG=0) TGT
									 WHERE ${DIM_JOIN_LIST})
								   OR EXISTS
								   (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST}, EFF_TO_DT										
										FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} 
										WHERE RCD_TMP_CLOSE_FLG=0) TGT
								   WHERE ${DIM_JOIN_LIST} AND SRC.EFF_FROM_DT > TGT.EFF_TO_DT )"


								   
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Keys required to insert records using temporary table"
   fi
   
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
   print_msg "###################################################################################"
   print_msg ""
   set_activity_count insert
   audit_log 3
   
   set_dimension_variable
}

#########################################################################################
# Function Name : close_using_temp
# Description   : This function closes the dimension records which are no longer active
#				  in the source system. The record close flag is updated from 0 to 1
#########################################################################################

function scd_close_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Closing deleted/stopped records in $TARGET_TABLE"
	if [[ -n $DIM_IDNT ]]
	then
		  CLOSE_DIMENSION_SQL="UPDATE ${TARGET_DB}.$TARGET_TABLE tgt
							   SET RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
								  ,RCD_CLOSE_FLG=1
								  ,RCD_CLOSE_DT=$(DATATYPE_CONV "'$DATEKEY'" DATE 'YYYYMMDD')
								  ,EFF_TO_DT =$(DATATYPE_CONV "'$DATEKEY'" DATE 'YYYYMMDD')-1
							   WHERE NOT EXISTS
								  (SELECT 1 FROM ${TEMP_DB}.$TEMP_TABLE src where ${DIM_JOIN_LIST} and tgt.RCD_CLOSE_FLG=0) 
									 AND $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') BETWEEN tgt.RCD_INS_TS AND tgt.RCD_CLOSE_DT"
									 
	   run_query -d "$TARGET_DB" -q "$CLOSE_DIMENSION_SQL" -m "Unable to Close Dimension Records"
	else
	   chk_err -r 1 -m "Keys required to close records using temporary table"
	fi
	print_msg "Closing deleted/stopped records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
}


###########################################################################################
# Function Name : scd_nonkey_close_and_insert_using_temp
# Description   : This function has no auto incrementing key defined for the dim table. It closes the old records and inserts new 
#				  records into the target table looking after the temporary table.
###########################################################################################

function scd_nonkey_close_and_insert_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
									FROM ${TARGET_DB}.$TARGET_TABLE TGT,
										 ${TEMP_DB}.${TEMP_TABLE}  SRC
									SET  EFF_TO_DT = SRC.EFF_FROM_DT - 1
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
										,RCD_CLOSE_FLG = 1
										,RCD_CLOSE_DT = $(DATATYPE_CONV "'$CURR_DAY'" DATE 'YYYY-MM-DD')
								    WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK} 
										AND (${DIM_IDNT_SRC_LIST},SRC.EFF_FROM_DT) IN (SELECT ${DIM_IDNT_SRC_LIST},MIN(SRC.EFF_FROM_DT) FROM ${TEMP_DB}.${TEMP_TABLE} SRC
											INNER JOIN ${VIEW_DB}.${VIEW_PREFIX}$TARGET_TABLE TGT ON ${DIM_JOIN_LIST}
											WHERE (${UPDATE_COLUMN_COMPARE_LIST}) ${DIMENSION_OPEN_CHECK} 
												AND SRC.EFF_FROM_DT > TGT.EFF_FROM_DT GROUP BY ${DIM_IDNT_SRC_LIST} )"
									
		    

			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
					
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3
	
	TEMP_TABLE_COLUMN_LIST=$(echo $TEMP_TABLE_COLUMN_LIST | sed 's/, ASSOC_CUS_ID//g')
	
	print_msg ""
	print_msg "###################################################################################"
    print_msg "Inserting new records into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
   if [[ -n $DIM_IDNT ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
								(    
								   ${DIM_IDNT_LIST}
								   , EFF_FROM_DT
								   , EFF_TO_DT
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , RCD_INS_TS
								   , RCD_UPD_TS
								   , RCD_CLOSE_FLG
								   , RCD_CLOSE_DT
							   )
							   
							   SELECT ${DIM_IDNT_LIST}
								   , EFF_FROM_DT
								   , EFF_TO_DT
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
								   , RCD_CLOSE_FLG
								   ,$(DATATYPE_CONV "'9999-12-31'" DATE 'YYYY-MM-DD')
							   
								FROM ${TEMP_DB}.${TEMP_TABLE}  SRC
								 WHERE NOT EXISTS
								  (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST} 
										FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} 
										WHERE RCD_CLOSE_FLG=0) TGT
									 WHERE ${DIM_JOIN_LIST})
								  OR EXISTS
								   (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST}, EFF_TO_DT										
										FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} 
										WHERE RCD_CLOSE_FLG=0) TGT
								   WHERE ${DIM_JOIN_LIST} AND SRC.EFF_FROM_DT > TGT.EFF_TO_DT )" 


								   
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Keys required to insert records using temporary table"
   fi
   
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
   print_msg "###################################################################################"
   print_msg ""
   set_activity_count insert
   audit_log 3
   
   set_dimension_variable
}


###########################################################################################
# Function Name : rcd_close_and_insert_using_temp
# Description   : This function close the old records and insert new records into the target table 
#                 looking after the temporary table.
###########################################################################################

function rcd_close_and_insert_using_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
									FROM ${TARGET_DB}.$TARGET_TABLE TGT,
										${TEMP_DB}.${TEMP_TABLE}  SRC
									SET  END_DAY_KEY = SRC.DAY_KEY - 1
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
									WHERE ${DIM_JOIN_LIST} 
										AND TGT.END_DAY_KEY = CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD')
										AND (SRC.${DIM_IDNT_LIST},SRC.DAY_KEY) IN (SELECT SRC.${DIM_IDNT_LIST},MIN(SRC.DAY_KEY) FROM ${TEMP_DB}.${TEMP_TABLE} SRC
											INNER JOIN ${VIEW_DB}.${VIEW_PREFIX}$TARGET_TABLE TGT ON ${DIM_JOIN_LIST}
												AND (${UPDATE_COLUMN_COMPARE_LIST})
												AND TGT.END_DAY_KEY = CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD')
											WHERE SRC.DAY_KEY > TGT.DAY_KEY GROUP BY SRC.${DIM_IDNT_LIST} )"

			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
					
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3

	print_msg ""
	print_msg "###################################################################################"
    print_msg "Inserting new records into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
   if [[ -n $DIM_IDNT ]]
   then
		INSERT_DIMENSION_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
								(   ${DIM_KEY_LIST}
								   , ${DIM_IDNT_LIST}
								   , DAY_KEY
								   , END_DAY_KEY
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , RCD_INS_TS
								   , RCD_UPD_TS
							   )							   
							   SELECT ${DIM_KEY_LIST} 
									,${DIM_IDNT_LIST}
								   , DAY_KEY
								   , END_DAY_KEY
								   , ${TEMP_TABLE_COLUMN_LIST}
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
								   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS							   
								FROM ${TEMP_DB}.${TEMP_TABLE}  SRC
								 WHERE NOT EXISTS
								  (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} QUALIFY DAY_KEY = MAX(DAY_KEY) OVER (PARTITION BY ${DIM_IDNT_LIST})) TGT
									 WHERE ${DIM_JOIN_LIST})
								   OR EXISTS
								   (SELECT 1 
									 FROM (SELECT ${DIM_IDNT_LIST}, END_DAY_KEY FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE} QUALIFY DAY_KEY = MAX(DAY_KEY) OVER (PARTITION BY ${DIM_IDNT_LIST})) TGT
								   WHERE ${DIM_JOIN_LIST} AND SRC.DAY_KEY > TGT.END_DAY_KEY )"
	
		run_query -d "$TARGET_DB" -q "$INSERT_DIMENSION_SQL" -m "Unable to Insert Dimension Records"
   else
      chk_err -r 1 -m "Keys required to insert records using temporary table"
   fi
   
   print_msg "Inserting new records into $TARGET_TABLE completed successfully"
   print_msg "###################################################################################"
   print_msg ""
   set_activity_count insert
   audit_log 3
}

###########################################################################################
# Function Name : update_using_temp_cdh
# Description   : This function updates the target table looking after the temporary table
###########################################################################################

function update_using_temp_cdh {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Updating records in $TARGET_TABLE"

		if [[ -n $DIM_IDNT ]]
		then
			 UPDATE_DIMENSION_SQL="UPDATE tgt
										FROM ${TARGET_DB}.$TARGET_TABLE tgt, 
										${TEMP_DB}.${TEMP_TABLE} src
								   SET ${UPDATE_COLUMNS_LIST}
										,RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
										${UPDATE_CLOSE_MAINTAINANCE_COLUMNS}
									WHERE ${DIM_JOIN_LIST} ${DIMENSION_OPEN_CHECK}
										AND (${UPDATE_COLUMN_COMPARE_LIST})"


			run_query -d "$TARGET_DB" -q "$UPDATE_DIMENSION_SQL" -m "Unable to update Dimension Records"
		else
			chk_err -r 1 -m "Keys required to update records using temporary table"
		fi
	print_msg "Updating records in $TARGET_TABLE completed successfully"
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count update
	audit_log 3   
}
