#!/bin/sh

####################################################################################################
# script : fact_functions.ksh                                                                      #
# Description : This library function holds the fact processing functions                          #
# Modifications                                                                                    #
# 12/27/2012  : Logic   : Initial Script                                                           #
# 05/07/2015  : mkhanal : changed GET_PRIMARY_AMOUNT 
####################################################################################################

########################## Standard fact processing functions started ##############################



####################################################################################################
# Function Name : insert_std_fact_from_temp
# Description   : This function loads records from temp table to standard fact target tables
####################################################################################################

function insert_std_fact_from_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Standard Fact from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TARGET_TABLE}
                  (       ${FACT_KEYS_LIST}
					      $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						, ${FACT_FIELDS_LIST}
						, RCD_INS_TS
						, RCD_UPD_TS 
				   )                           
					SELECT ${FACT_KEYS_LIST}
					       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					     , ${FACT_FIELDS_LIST}
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE ( ${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}


####################################################################################################
# Function Name : insert_std_fact_from_temp_sls
# Description   : This function loads records from temp table to standard fact target tables
#				  Other fact function don't add TXN_ID and TXN_LN_ID and also non-incremental fact 
#				  field lists like unit price which are not to go under incremental update are handled
#				  by this function.	
#				  The comma has been removed from ${NON_INCREMENTAL_FACT_FIELDS_LIST} as this field does
#				  not occur in Transaction Discount
####################################################################################################

function insert_std_fact_from_temp_sls {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Standard Fact from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE		
                  (       ${FACT_KEYS_LIST}
				        , TXN_LN_KEY
						, ${OTHER_FIELDS_LIST}
						, ${FACT_FIELDS_LIST}
						  ${NON_INCREMENTAL_FACT_FIELDS_LIST}
						, RCD_INS_TS
						, RCD_UPD_TS
				   )                           
					SELECT				
					      ${FACT_KEYS_LIST}
						, COALESCE((SELECT MAX(TXN_LN_KEY) FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE}), 0) + RANK() OVER (ORDER BY ${FACT_KEYS_LIST})  
					    , ${OTHER_FIELDS_LIST}
					    , ${FACT_FIELDS_LIST}
						  ${NON_INCREMENTAL_FACT_FIELDS_LIST}
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE (${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}

####################################################################################################
# Function Name : insert_agg_std_fact_from_temp_sls
# Description   : This function loads records from temp table to standard aggregate fact target table for sales.
#				  Fact fields like unit price which are not to go under incremental update are handled
#				  by this function.
#				  The comma has been removed from ${NON_INCREMENTAL_FACT_FIELDS_LIST}
#				  and ${TXN_AGG_KEYS_LIST} as this field does not occur in Transaction Discount
####################################################################################################
function insert_agg_std_fact_from_temp_sls {

	if [[ -n $TXN_AGG_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TXN_AGG_TABLE} 
                  (${TXN_AGG_FIELDS_LIST} 
				   $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
				   $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				 , ${FACT_FIELDS_LIST}
				   $(if [[ -n  ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo " ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_FIELDS}"; fi)
                 , RCD_INS_TS
                 , RCD_UPD_TS
               )                           
               SELECT ${TXN_AGG_FIELDS_LIST}
			          $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
       			      $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				    , ${FACT_AGG_FIELDS_LIST}
				      $(if [[ -n  ${NON_INCREMENTAL_FACT_AGG_FIELDS_LIST} ]]; then echo ",  ${NON_INCREMENTAL_FACT_AGG_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_AGG_FIELDS_LIST}"; fi)
                    , MAX($(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) RCD_INS_TS
                    , MAX($(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) RCD_UPD_TS
                 FROM ${TEMP_DB}.${TEMP_TABLE} src
                WHERE ( ${TXN_AGG_KEYS_LIST} ) NOT IN ( SELECT ${TXN_AGG_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TXN_AGG_TABLE})
				GROUP BY  ${TXN_AGG_FIELDS_LIST}  $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
				  $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${TXN_AGG_TABLE}
	
}

####################################################################################################
# Function Name : insert_pos_fact_from_temp_to_agg
# Description   : This function is to load the inventory data aggregated in temporary table to the target table.
#                 Group by clause in not used here, rather its done while loading the temporary table,
#                 also the record from the target table should be deleted before this function is called
#                 for rows with same primary key.
####################################################################################################
function insert_pos_fact_from_temp_to_agg {

	if [[ -n $TXN_AGG_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TXN_AGG_TABLE} 
                  (${TXN_AGG_FIELDS_LIST} 
				   $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
				   $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				 , ${FACT_FIELDS_LIST}
				   $(if [[ -n  ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo " ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_FIELDS}"; fi)
                 , RCD_INS_TS
                 , RCD_UPD_TS
               )                           
               SELECT ${TXN_AGG_FIELDS_LIST}
			          $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
       			      $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				    , ${FACT_FIELDS_LIST}
				      $(if [[ -n  ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo " ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_FIELDS}"; fi)
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                 FROM ${TEMP_DB}.${TEMP_TABLE} src"
                
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${TXN_AGG_TABLE}  
	
}



####################################################################################################
# Function Name : insert_std_fact_from_temp_agg
# Description   : This function is to load the data aggregated in temporary table to the target table.
#                 Group by clause in not used here, rather its done while loading the temporary table. 
####################################################################################################
function insert_std_fact_from_temp_agg {

	if [[ -n $TXN_AGG_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TXN_AGG_TABLE} 
                  (${TXN_AGG_FIELDS_LIST} 
				   $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
				   $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				 , ${FACT_FIELDS_LIST}
				   $(if [[ -n  ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo " ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_FIELDS}"; fi)
                 , RCD_INS_TS
                 , RCD_UPD_TS
               )                           
               SELECT ${TXN_AGG_FIELDS_LIST}
			          $(if [[ -n ${TXN_AGG_FIELDS_LIST} ]]; then echo ", ${TXN_AGG_KEYS_LIST}" ;else echo "${TXN_AGG_KEYS_LIST}"; fi)
       			      $(if [[ -n ${TXN_AGG_OTHER_FIELDS} ]]; then echo ", ${TXN_AGG_OTHER_FIELDS_LIST}" ;else echo "${TXN_AGG_OTHER_FIELDS_LIST}"; fi)
				    , ${FACT_FIELDS_LIST}
				        $(if [[ -n  ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo " ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo " ${NON_INCREMENTAL_FACT_FIELDS}"; fi)
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                 FROM ${TEMP_DB}.${TEMP_TABLE} src
                WHERE ( ${TXN_AGG_KEYS_LIST} ) NOT IN ( SELECT ${TXN_AGG_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TXN_AGG_TABLE})
				"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${TXN_AGG_TABLE}
	
}



####################################################################################################
# Function Name : insert_agg_std_fact_from_temp
# Description   : This function loads records from temp table to standard aggregate fact target tables
####################################################################################################
function insert_agg_std_fact_from_temp {

	if [[ -n $TXN_AGG_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TXN_AGG_TABLE} 
                  (  ${TXN_AGG_KEYS_LIST}
				, ${TXN_AGG_FIELDS_LIST}
				, ${TXN_AGG_OTHER_FIELDS_LIST}
                , ${FACT_FIELDS_LIST}
                , RCD_INS_TS
                , RCD_UPD_TS
               )                           
               SELECT ${TXN_AGG_KEYS_LIST}
				   , ${TXN_AGG_FIELDS_LIST}
				   , ${TXN_AGG_OTHER_FIELDS_LIST}
                   , ${FACT_AGG_FIELDS_LIST}
                   , MAX($(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) RCD_INS_TS
                   , MAX($(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) RCD_UPD_TS
                 FROM ${TEMP_DB}.${TEMP_TABLE} src
                WHERE ( ${TXN_AGG_KEYS_LIST} ) NOT IN ( SELECT ${TXN_AGG_KEYS_LIST} FROM  ${VIEW_DB}.${VIEW_PREFIX}${TXN_AGG_TABLE})
				GROUP BY ${TXN_AGG_KEYS_LIST}, ${TXN_AGG_FIELDS_LIST}, ${TXN_AGG_OTHER_FIELDS_LIST}
				"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${TXN_AGG_TABLE}
	
}

#########################################################################################################
# Function Name : incremental_update_from_temp
# Description   : This function incremental updates records in target table using data in temporary table
#########################################################################################################
function incremental_update_from_temp {

	if [[ -n $FACT_KEYS ]]
	then
		UPDATE_SQL="UPDATE tgt
                 FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET  
                   ${INCR_FACT_FIELDS_LIST}
		           $(if [[ -n ${UPDATE_OTHER_FIELDS_LIST} ]]; then echo ", ${UPDATE_OTHER_FIELDS_LIST}" ;else echo "${UPDATE_OTHER_FIELDS_LIST}"; fi)
                   , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                WHERE ${FACT_JOIN_LIST}"
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 
}

#########################################################################################################
# Function Name : incremental_agg_update_from_temp
# Description   : This function incremental aggregate updates records in target table using data in temporary table
#########################################################################################################
function incremental_agg_update_from_temp {

	if [[ -n $TXN_AGG_KEYS ]] 
	then
		UPDATE_SQL="UPDATE tgt
                 FROM ${TARGET_DB}.${TXN_AGG_TABLE} tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET ${UPDATE_TXN_AGG_FIELDS}
				       $(if [[ -n ${UPDATE_TXN_AGG_FIELDS} ]]; then echo ", ${INCR_FACT_FIELDS_LIST}" ;else echo "${INCR_FACT_FIELDS_LIST}"; fi)
					   $(if [[ -n ${UPDATE_NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo ", ${UPDATE_NON_INCREMENTAL_FACT_FIELDS_LIST}"; fi)
                   , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                WHERE ${TXN_AGG_JOIN_LIST}"
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update fact records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${TXN_AGG_TABLE}
}

###########################################################################################################
# Function Name : update_std_fact_from_temp
# Description   : This function updates records in standard fact target table using data in temporary table
###########################################################################################################

function update_std_fact_from_temp {

	if [[ -n $FACT_KEYS ]]
	then
		UPDATE_SQL="UPDATE tgt
                  FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_LIST} ," ;else echo "${UPDATE_OTHER_FIELDS_LIST}"; fi)
                     ${UPDATE_FACT_FIELDS_LIST}
                     , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                  WHERE ${FACT_JOIN_LIST}
                    AND ($(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_CHECK} OR " ;else echo "${UPDATE_OTHER_FIELDS_CHECK}"; fi) ${UPDATE_FACT_FIELDS_CHECK})
                "
                
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 
	
}

########################## Standard fact processing functions ended ################################


######################### Positional fact processing functions started ###############################

################################################################################################################
# Function Name : close_pos_fact_from_temp
# Description   : This function closes positional fact records in target table, if new record for exisitng fact 
#				  keys exist in temp table. Record close flag is set to 1 and end_day_key is set to current day 
################################################################################################################

function close_pos_fact_from_temp {

	if [[ -n $FACT_KEYS ]]
	then
   
		# Close the records if DIM keys matches and date key does not match
   
		UPDATE_SQL="UPDATE tgt
              FROM ${TARGET_DB}.${TARGET_TABLE} tgt, ${TEMP_DB}.${TEMP_TABLE} src
               SET END_DAY_KEY=src.DAY_KEY - 1
                 , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
              WHERE ${FACT_JOIN_LIST}
               AND tgt.DAY_KEY <> src.DAY_KEY AND TGT.END_DAY_KEY='9999-12-31'"    

		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	  
	  
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi

	set_activity_count update
	audit_log 3 
}

################################################################################################################
# Function Name : close_pos_fact_from_temp
# Description   : This function closes positional fact records in target table, if new record for exisitng fact 
#				  keys exist in temp table. Record close flag is set to 1 and end_day_key is set to current day 
################################################################################################################

function close_pos_fact_from_temp_using_collist {

	if [[ -n $FACT_KEYS ]]
	then
   
		# Close the records if DIM keys matches and predefined list of columns doesn't match
   
		UPDATE_SQL="UPDATE tgt
              FROM ${TARGET_DB}.${TARGET_TABLE} tgt, ${TEMP_DB}.${TEMP_TABLE} src
               SET END_DAY_KEY=src.DAY_KEY - 1
                 , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
              WHERE ${FACT_JOIN_LIST}
               AND ${CHANGE_TRACK_JOIN_LIST}
               AND tgt.DAY_KEY <> src.DAY_KEY AND TGT.END_DAY_KEY='9999-12-31'"    

		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	  
	  
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi

	set_activity_count update
	audit_log 3 
}

################################################################################################################
# Function Name : insert_pos_fact_from_temp
# Description   : This function loads new records into positional fact table using records from temp table 
################################################################################################################

function insert_pos_fact_from_temp { 

	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
                  (  ${FACT_KEYS_LIST}
                , DAY_KEY
                , END_DAY_KEY
                 ${TARGET_TABLE_FIELDS_LIST}
                , ${FACT_FIELDS_LIST}
                , RCD_INS_TS
                , RCD_UPD_TS
                  )                           
               SELECT ${FACT_KEYS_LIST}
                   , DAY_KEY
                   , $(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD') END_DAY_KEY
                    ${TARGET_TABLE_FIELDS_LIST}
                   , ${FACT_FIELDS_LIST}
                   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
                   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                   FROM ${TEMP_DB}.${TEMP_TABLE} src
                 WHERE (${FACT_KEYS_LIST}, DAY_KEY) NOT IN (SELECT ${FACT_KEYS_LIST}, DAY_KEY FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})"
                 
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 

}

################################################################################################################
# Function Name : insert_pos_fact_with_surrog_from_temp
# Description   : This function loads new records into positional fact table with surrogate key using records from temp table 
################################################################################################################

function insert_pos_fact_with_surrog_from_temp { 

	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE 
                  (  ${FACT_KEYS_LIST}
                , ${FACT_SURROGATE_KEY}
                , DAY_KEY
                , END_DAY_KEY
                 ${TARGET_TABLE_FIELDS_LIST}
                , ${FACT_FIELDS_LIST}
                , RCD_INS_TS
                , RCD_UPD_TS
                  )                           
               SELECT ${FACT_KEYS_LIST}
                   , COALESCE((SELECT MAX(${FACT_SURROGATE_KEY}) FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE}), 0) + RANK() OVER (ORDER BY ${FACT_KEYS_LIST})
                   , DAY_KEY
                   , $(DATATYPE_CONV "'99991231'" DATE 'YYYYMMDD') END_DAY_KEY
                    ${TARGET_TABLE_FIELDS_LIST}
                   , ${FACT_FIELDS_LIST}
                   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
                   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                   FROM ${TEMP_DB}.${TEMP_TABLE} src
                 WHERE (${FACT_KEYS_LIST}, DAY_KEY) NOT IN (SELECT ${FACT_KEYS_LIST}, DAY_KEY FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})"
                 
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 

}

####################################################################################################################
# Function Name : delete_cur_table_using_temp
# Description   : This function deletes exisiting data from CUR fact tables, whose keys exist in temporary table
####################################################################################################################

function delete_cur_table_using_temp {

	if [[ -n $FACT_KEYS ]]
	then
		DELETE_SQL="DELETE FROM ${TARGET_DB}.${CUR_TABLE} tgt
                    WHERE EXISTS (SELECT 1 FROM ${TEMP_DB}.${TEMP_TABLE} src
                                    WHERE $FACT_JOIN_LIST)"
   
  
		run_query -d "${TARGET_DB}" -q "${DELETE_SQL}" -m "Unable to delete current Records"
	else
		chk_err -r 1 -m "Keys required to delete records using temporary table"
	fi
	
	set_activity_count delete 
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${CUR_TABLE}
}

####################################################################################################################
# Function Name : insert_cur_table_using_temp
# Description   : This function loads records into CUR fact tables using records from temporary table
####################################################################################################################

function insert_cur_table_using_temp {

	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$CUR_TABLE 
				(  	${FACT_KEYS_LIST}
					${CUR_TABLE_FIELDS_LIST}
					, ${FACT_FIELDS_LIST}
					, RCD_INS_TS
					, RCD_UPD_TS
				)                           
				SELECT ${FACT_KEYS_LIST}
                    ${CUR_TABLE_FIELDS_LIST}
                   , ${FACT_FIELDS_LIST}
                   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
				   , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                FROM ${TEMP_DB}.${TEMP_TABLE}
                WHERE (${FACT_KEYS_LIST}) NOT IN (SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${CUR_TABLE})"    
      
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to insert current Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${CUR_TABLE}
}

########################################################################################################################
# Function Name : insert_cur_table_using_rev_with_surrrog
# Description   : This function loads records into CUR fact tables using records from revision table with surrogate key
########################################################################################################################

function insert_cur_table_using_rev_with_surrrog {

	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$CUR_TABLE 
				(  	${FACT_KEYS_LIST}
					${CUR_TABLE_FIELDS_LIST}
					, ${FACT_FIELDS_LIST}
					, RCD_INS_TS
					, RCD_UPD_TS
				)                           
				SELECT ${FACT_KEYS_LIST}
                    ${CUR_TABLE_FIELDS_LIST}
                    , ${FACT_FIELDS_LIST}
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
                    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
                FROM ${TARGET_DB}.${TARGET_TABLE}
                WHERE (${FACT_KEYS_LIST}) NOT IN (SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${CUR_TABLE})
                AND END_DAY_KEY='9999-12-31'"    
      
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to insert current Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	
	set_activity_count insert
	audit_log 3 ${TEMP_DB} ${TEMP_TABLE} ${TARGET_DB} ${CUR_TABLE}
}
####################################################################################################################
# Function Name : insert_cur_table_using_temp_with_surrog
# Description   : This function loads records into CUR fact tables using records from revision table with surrogate key.
####################################################################################################################

function insert_cur_table_using_temp_with_surrog {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Positional Fact with Surrogate Key from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${CUR_TABLE}		
                  (       ${FACT_KEYS_LIST}
				        ,DAY_KEY
						,${FACT_SURROGATE_KEY}
						$(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						$(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
						$(if [[ -n ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo ", ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo "${NON_INCREMENTAL_FACT_FIELDS_LIST}"; fi)
						, RCD_INS_TS
						, RCD_UPD_TS
				   )                           
					SELECT				
					    ${FACT_KEYS_LIST}
						,DAY_KEY
						,COALESCE((SELECT MAX(${FACT_SURROGATE_KEY}) FROM ${TARGET_DB}.${CUR_TABLE}), 0) + RANK() OVER (ORDER BY ${FACT_KEYS_LIST})  
					    $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					    $(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
						$(if [[ -n ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo ", ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo "${NON_INCREMENTAL_FACT_FIELDS_LIST}"; fi)
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE (${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${CUR_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}
###########################################################################################################
# Function Name : update_cur_table_from_temp
# Description   : This function updates records in CUR table using data in temporary table
###########################################################################################################

function update_cur_table_from_temp {

	if [[ -n $FACT_KEYS ]]
	then
		UPDATE_SQL="UPDATE tgt
                  FROM ${TARGET_DB}.${CUR_TABLE} tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_LIST} ," ;else echo "${UPDATE_OTHER_FIELDS_LIST}"; fi)
                     ${UPDATE_FACT_FIELDS_LIST}
                     , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                  WHERE ${FACT_JOIN_LIST}
                    AND ($(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_CHECK} OR " ;else echo "${UPDATE_OTHER_FIELDS_CHECK}"; fi) ${UPDATE_FACT_FIELDS_CHECK})
                "
                
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 
	
}


####################################################################################################################
# Function Name : close_fact_from_dimension
# Description   : This function will close the fact records if the dimension is closed.
#                 in case of current table the record is deleted else the end_day_key is updated
####################################################################################################################


function close_fact_from_dimension {

   ret_code=0
   query_err_msg="Running Query failed"
   query_db=""
   query_command=""
   query_file=""
   CURR_DAY='2015-05-05'  # will be extracted from the table 

   while getopts "d:t:c:p:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       t )  table_name=$OPTARG;;

       c )  current=$OPTARG;;
	   
	   p )  table_key_parameter=$OPTARG;;

       \? ) chk_err -r 1 -m 'run_query usage: -d database -q query -m err_msg'

      esac
   done

   
   if [[ "$query_db" == "" ]]
   then
       chk_err -r 1 -m "No database passed to run_query function"
   fi
   
   if [[ "$table_name" == "" ]]
   then
       chk_err -r 1 -m "No fact table  passed to run close operation"
   fi
   
   if [[ "$current" == "" ]]
   then
      chk_err -r 1 -m "Specify if the table is current table or not"
   fi
   
   if [[ "$table_key_parameter" == "" ]]
   then
      chk_err -r 1 -m "Specify the Dimension identifier in the fact table"
   fi


   
TARGET_DB=$query_db
TARGET_TABLE=$table_name
DIM_IDNT=$table_key_parameter

CHECK=1


if [[ $current = 'Y' || $current = 'y' ]] 
then

	FACT_CLOSE_QUERY="DELETE FROM ${TARGET_DB}.${TARGET_TABLE} WHERE "

    TABLE=1
 for field in $DIM_IDNT
   do
  
	 if [[ $CHECK -eq 1 ]]
     then	 
	      if [[ $TABLE -eq 1 ]]
	      then
            
             TABLE_NAME=$field
             TABLE=0
          else
             FACT_CLOSE_QUERY="${FACT_CLOSE_QUERY} $field IN (SELECT $field FROM ${TARGET_DB}.${TABLE_NAME} WHERE RCD_CLOSE_FLG=1) "
             TABLE=1
			 CHECK=0
          fi
     else
	 
	 if [[ $TABLE -eq 1 ]]
	      then
             TABLE_NAME=$field
             CHECK=0
		     TABLE=0
          else
             FACT_CLOSE_QUERY="${FACT_CLOSE_QUERY} OR $field IN (SELECT $field FROM ${TARGET_DB}.${TABLE_NAME} WHERE RCD_CLOSE_FLG=1)"
             TABLE=1
			 CHECK=0
          fi
	 fi
      
   done
   
   echo $FACT_CLOSE_QUERY
   
else 
   
   
   FACT_CLOSE_QUERY="UPDATE ${TARGET_DB}.${TARGET_TABLE} 
                  SET END_DAY_KEY='$CURR_DAY' WHERE "

    TABLE=1
 for field in $DIM_IDNT
   do
  
	 if [[ $CHECK -eq 1 ]]
     then	 
	      if [[ $TABLE -eq 1 ]]
	      then
            
             TABLE_NAME=$field
             TABLE=0
          else
             FACT_CLOSE_QUERY="${FACT_CLOSE_QUERY} $field IN (SELECT $field FROM ${TARGET_DB}.${TABLE_NAME} WHERE RCD_CLOSE_FLG=1) "
             TABLE=1
			 CHECK=0
          fi
     else
	 
	 if [[ $TABLE -eq 1 ]]
	      then
             TABLE_NAME=$field
             CHECK=0
		     TABLE=0
          else
             FACT_CLOSE_QUERY="${FACT_CLOSE_QUERY} OR $field IN (SELECT $field FROM ${TARGET_DB}.${TABLE_NAME} WHERE RCD_CLOSE_FLG=1)"
             TABLE=1
			 CHECK=0
          fi
	 fi
      
   done
   
   echo $FACT_CLOSE_QUERY
   
fi
run_query -d "$TARGET_DB" -q "$FACT_CLOSE_QUERY" -m "Unable to Close Fact Records"
}

####################################################################################################################
# Function Name : close_positional_fact
# Description   : This function will close all the open facts from the positional fact tables (both compressed and current table).
####################################################################################################################
function close_positional_fact
{
	CLOSE_DATE=$1
	
	FACT_CURR_TABLE="${TARGET_TABLE}"
	FACT_COMPRESSED_TABLE="${CUR_TABLE}"
	
	CLOSE_QUERY1="UPDATE ${TARGET_DB}.${TARGET_TABLE}
			SET END_DAY_KEY = $(DATATYPE_CONV "'$CLOSE_DATE'" DATE 'YYYYMMDD')
			WHERE END_DAY_KEY='9999-12-31' "
	
	run_query -d "$TARGET_DB" -q "$CLOSE_QUERY1" -m "Unable to close records in compressed table"
	
	CLOSE_QUERY2="DELETE FROM ${TARGET_DB}.${CUR_TABLE}"
	
	run_query -d "$TARGET_DB" -q "$CLOSE_QUERY2" -m "Unable to close records in current table"
}

########################## Positional fact processing functions ended ################################


function insert_fact_with_surrog_key_from_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Standard Fact with Surrogate Key from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.$TARGET_TABLE		
                  (       ${FACT_KEYS_LIST}
				        , ${FACT_SURROGATE_KEY}
						$(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						$(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
						$(if [[ -n ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo ", ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo "${NON_INCREMENTAL_FACT_FIELDS_LIST}"; fi)
						, RCD_INS_TS
						, RCD_UPD_TS
				   )                           
					SELECT				
					      ${FACT_KEYS_LIST}
						, COALESCE((SELECT MAX(${FACT_SURROGATE_KEY}) FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE}), 0) + RANK() OVER (ORDER BY ${FACT_KEYS_LIST})  
					    $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					    $(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
						$(if [[ -n ${NON_INCREMENTAL_FACT_FIELDS_LIST} ]]; then echo ", ${NON_INCREMENTAL_FACT_FIELDS_LIST}" ;else echo "${NON_INCREMENTAL_FACT_FIELDS_LIST}"; fi)
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					    , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE (${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}

####################################################################################################################
# Function Name : set_fact_variable
# Description   : This function sets all variables required for fact loading
####################################################################################################################

function set_fact_variable
{
   
   CHECK=1
   for field in $FACT_KEYS
   do
      if [[ $CHECK -eq 1 ]]
      then
         FACT_JOIN_LIST="src.$field=tgt.$field"
		 LCP_JOIN_LIST="src.$field=lcp.$field"
         FACT_KEYS_LIST="$field"
         CHECK=0
      else
         FACT_JOIN_LIST="${FACT_JOIN_LIST} AND src.$field=tgt.$field"
         LCP_JOIN_LIST="${LCP_JOIN_LIST} AND src.$field=lcp.$field"
         FACT_KEYS_LIST="${FACT_KEYS_LIST}, $field"
      fi
   done
   
   
   CHECK=1
   for field in $FACT_IDNT
   do
      if [[ $CHECK -eq 1 ]]
      then
         FACT_IDNT_LIST="$field"
         CHECK=0
      else
         FACT_IDNT_LIST="${FACT_IDNT_LIST}, $field"
      fi
   done
   
   
    CHECK=1
   for field in $TXN_AGG_KEYS
   do
      if [[ $CHECK -eq 1 ]]
      then
		TXN_AGG_JOIN_LIST="src.$field=tgt.$field"
        TXN_AGG_KEYS_LIST="$field"
         CHECK=0
      else
		 TXN_AGG_JOIN_LIST="$TXN_AGG_JOIN_LIST AND src.$field=tgt.$field"
         TXN_AGG_KEYS_LIST="${TXN_AGG_KEYS_LIST}, $field"
      fi
   done
   
   CHECK=1
   for field in $TXN_AGG_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
		UPDATE_TXN_AGG_FIELDS="$field=src.$field"
        TXN_AGG_FIELDS_LIST="$field"
         CHECK=0
      else
		 UPDATE_TXN_AGG_FIELDS="$UPDATE_TXN_AGG_FIELDS, $field=src.$field"
         TXN_AGG_FIELDS_LIST="${TXN_AGG_FIELDS_LIST}, $field"
      fi
   done
   
   
      CHECK=1
   for field in $FACT_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        FACT_AGG_FIELDS_LIST="sum(${field})"
         CHECK=0
      else
         FACT_AGG_FIELDS_LIST="${FACT_AGG_FIELDS_LIST},sum(${field})"
      fi
   done

      CHECK=1
   for field in $NON_INCREMENTAL_FACT_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        NON_INCREMENTAL_FACT_AGG_FIELDS_LIST="MAX(${field})"
         CHECK=0
      else
         NON_INCREMENTAL_FACT_AGG_FIELDS_LIST="${NON_INCREMENTAL_FACT_AGG_FIELDS_LIST},MAX(${field})"
      fi
   done
   
      CHECK=1
   for field in $FACT_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        FACT_FIELDS_LIST="$field"
         CHECK=0
      else
         FACT_FIELDS_LIST="${FACT_FIELDS_LIST}, $field"
      fi
   done
   
     CHECK=1
   for field in $NON_INCREMENTAL_FACT_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        NON_INCREMENTAL_FACT_FIELDS_LIST=",$field"
		UPDATE_NON_INCREMENTAL_FACT_FIELDS_LIST="$field=src.$field"
        UPDATE_NON_INCREMENTAL_FACT_FIELDS_CHECK="coalesce(tgt.$field, 0) <> coalesce(src.$field,0)"
         CHECK=0
      else
         NON_INCREMENTAL_FACT_FIELDS_LIST="${NON_INCREMENTAL_FACT_FIELDS_LIST}, $field"
		 UPDATE_NON_INCREMENTAL_FACT_FIELDS_LIST="${UPDATE_NON_INCREMENTAL_FACT_FIELDS_LIST}, $field=src.$field"
         UPDATE_NON_INCREMENTAL_FACT_FIELDS_CHECK="${UPDATE_NON_INCREMENTAL_FACT_FIELDS_CHECK} OR coalesce(tgt.$field, 0) <> coalesce(src.$field,0)"
      fi
   done
   
   CHECK=1
   for field in $OTHER_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        OTHER_FIELDS_LIST="$field"
         CHECK=0
      else
         OTHER_FIELDS_LIST="${OTHER_FIELDS_LIST}, $field"
      fi
   done
   
   CHECK=1
   for field in $TXN_AGG_OTHER_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then
        TXN_AGG_OTHER_FIELDS_LIST="$field"
         CHECK=0
      else
         TXN_AGG_OTHER_FIELDS_LIST="${TXN_AGG_OTHER_FIELDS_LIST}, $field"
      fi
   done
   
   CHECK=1
   for field in $OTHER_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
      then 
         OTHER_FIELDS_LIST="$field"
         UPDATE_OTHER_FIELDS_LIST="$field=src.$field"
         if [[ $field == *_DT* || $field == *_TS* ]] 
         then
            UPDATE_OTHER_FIELDS_CHECK="COALESCE(src.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS'))"
         else
            UPDATE_OTHER_FIELDS_CHECK="COALESCE(src.$field,'0') <> COALESCE(tgt.$field,'0')"
         fi
         CHECK=0 
      else 
         OTHER_FIELDS_LIST="${OTHER_FIELDS_LIST}, $field"
         UPDATE_OTHER_FIELDS_LIST="$UPDATE_OTHER_FIELDS_LIST, $field=src.$field"
         if [[ $field == *_DT* || $field == *_TS* ]] 
         then
            UPDATE_OTHER_FIELDS_CHECK="${UPDATE_OTHER_FIELDS_CHECK} OR COALESCE(src.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS'))"
         else
            UPDATE_OTHER_FIELDS_CHECK="${UPDATE_OTHER_FIELDS_CHECK} OR COALESCE(src.$field,'0') <> COALESCE(tgt.$field,'0')"
         fi
      fi
   done

   CHECK=1
   for field in $FACT_FIELDS
   do
      if [[ $CHECK -eq 1 ]]
         then 
         FACT_FIELDS_LIST="$field"
         INCR_FACT_FIELDS_LIST="$field=coalesce(tgt.$field,0) + coalesce(src.$field,0)"
         UPDATE_FACT_FIELDS_LIST="$field=src.$field"
         UPDATE_FACT_FIELDS_CHECK="coalesce(tgt.$field, 0) <> coalesce(src.$field,0)"
         CHECK=0 
      
      else
         FACT_FIELDS_LIST="$FACT_FIELDS_LIST, $field"
         INCR_FACT_FIELDS_LIST="${INCR_FACT_FIELDS_LIST}, $field = coalesce(tgt.$field,0) + coalesce(src.$field,0)"
         UPDATE_FACT_FIELDS_LIST="${UPDATE_FACT_FIELDS_LIST}, $field=src.$field"
         UPDATE_FACT_FIELDS_CHECK="${UPDATE_FACT_FIELDS_CHECK} OR coalesce(tgt.$field, 0) <> coalesce(src.$field,0)"
         
      fi
   done
   
   CHECK=1
   for field in $CHANGE_TRACK_IDNT
   do
      if [[ $CHECK -eq 1 ]]
      then
         if [[ $field == *_DT* || $field == *_TS* ]] 
         then
            CHANGE_TRACK_JOIN_LIST="COALESCE(src.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS'))"
         else
            CHANGE_TRACK_JOIN_LIST="COALESCE(src.$field,'0') <> COALESCE(tgt.$field,'0')"
         fi
         CHECK=0
      else
         if [[ $field == *_DT* || $field == *_TS* ]] 
         then
            CHANGE_TRACK_JOIN_LIST="($CHANGE_TRACK_JOIN_LIST OR COALESCE(src.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) <> COALESCE(tgt.$field, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')))"
         else
            CHANGE_TRACK_JOIN_LIST="($CHANGE_TRACK_JOIN_LIST OR COALESCE(src.$field,'0') <> COALESCE(tgt.$field,'0'))"
         fi
      fi
   done
   
   TARGET_TABLE_FIELDS_LIST=''
   TARGET_TABLE_FIELDS_ONLY_LIST=''
   CUR_TABLE_ONLY_FIELDS_LIST=''
   CUR_TABLE_FIELDS_LIST=''
   
   CUR_TABLE_FIELDS_CHECK=''
   for field in $TARGET_TABLE_ONLY_FIELDS
   do
      TARGET_TABLE_FIELDS_ONLY_LIST="${TARGET_TABLE_FIELDS_ONLY_LIST}, ${field}"
   done
   
   for field in $CUR_TABLE_ONLY_FIELDS
   do
      CUR_TABLE_ONLY_FIELDS_LIST="${CUR_TABLE_ONLY_FIELDS_LIST}, ${field}"
      CUR_TABLE_FIELDS_CHECK="${CUR_TABLE_FIELDS_CHECK} OR tgt.${field} <> src.${field}"
   done
   
   if [[ -n "$OTHER_FIELDS" ]]; then
      TARGET_TABLE_FIELDS_LIST="${TARGET_TABLE_FIELDS_ONLY_LIST}, ${OTHER_FIELDS_LIST}"
      CUR_TABLE_FIELDS_LIST="${CUR_TABLE_ONLY_FIELDS_LIST}, ${OTHER_FIELDS_LIST}"
      CUR_TABLE_FIELDS_CHECK="${CUR_TABLE_FIELDS_CHECK} OR ${UPDATE_OTHER_FIELDS_CHECK}"
   else
      TARGET_TABLE_FIELDS_LIST="${TARGET_TABLE_FIELDS_ONLY_LIST}"
      CUR_TABLE_FIELDS_LIST="${CUR_TABLE_ONLY_FIELDS_LIST}"
      CUR_TABLE_FIELDS_CHECK="${CUR_TABLE_FIELDS_CHECK}"
   fi
   return
}


####################################################################################################################
# Function Name : GET_PRIMARY_AMOUNT
# Description   : This function returns amount in global currency for the provided amount in local currency.
####################################################################################################################

function GET_PRIMARY_AMOUNT {
	LOCAL_AMOUNT=$1
	if [[ -n $2 ]]
	then 
		CNCY_CDE=$2
	else 
		CNCY_CDE="LOC.CNCY_CDE"
	fi
	if [[ -n $3 ]]
	then 
		AGG_FUNCTION=$3
	else 
		AGG_FUNCTION="SUM"
	fi
	if [[ -n $4 ]]
	then 
		DAY_ID=$4
	else 
		DAY_ID="SRC.DAY_ID"
	fi	
	GLOBAL_AMOUNT="CASE WHEN MAX(${CNCY_CDE}) = '${Primary_Currrency}'
                                                        THEN ${AGG_FUNCTION}(${LOCAL_AMOUNT})
                                                    ELSE
														${AGG_FUNCTION}(CAST((${LOCAL_AMOUNT}) AS DECIMAL(26,4))*(SELECT EXCH_RATE FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_F_EXCH_RATE_LU EXCH1
														WHERE  EXCH1.FROM_CNCY_CDE=${CNCY_CDE}
														AND EXCH1.TO_CNCY_CDE = '${Primary_Currrency}'
														AND ( ${DAY_ID} BETWEEN EXCH1.EFF_FROM_DT AND EXCH1.EFF_TO_DT)
														))
                            END "
                echo "${GLOBAL_AMOUNT}"
	
} 


###########################################################################################################
# Function Name : update_factless_fact_from_temp
# Description   : This function updates records in fact-less fact target table using data in temporary table
###########################################################################################################

function update_factless_fact_from_temp {

	if [[ -n $FACT_KEYS ]]
	then
		UPDATE_SQL="UPDATE tgt
                  FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_LIST} ," ;else echo "${UPDATE_OTHER_FIELDS_LIST}"; fi)
                      RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                  WHERE ${FACT_JOIN_LIST}
                    AND (${UPDATE_OTHER_FIELDS_CHECK})
                "
                
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 
	
}

########################## Standard fact processing functions ended ################################

####################################################################################################
# Function Name : insert_factless_fact_from_temp
# Description   : This function loads records from temp table to fact-less fact target tables
####################################################################################################

function insert_factless_fact_from_temp {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Fact-less Fact from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TARGET_TABLE}
                  (       ${FACT_KEYS_LIST}
					      $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						, RCD_INS_TS
						, RCD_UPD_TS 
				   )                           
					SELECT ${FACT_KEYS_LIST}
					       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE ( ${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}


####################################################################################################
# Function Name : insert_std_fact_from_temp_cdh
# Description   : This function loads records from temp table to standard fact target tables
####################################################################################################

function insert_std_fact_from_temp_cdh {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Standard Fact from Temp Tables"
	if [[ -n $FACT_KEYS ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TARGET_TABLE}
                  (       ${FACT_KEYS_LIST}
					      $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						, ${FACT_FIELDS_LIST}
						, RCD_INS_TS
						, RCD_UPD_TS 
				   )                           
					SELECT ${FACT_KEYS_LIST}
					       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					     , ${FACT_FIELDS_LIST}
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_INS_TS
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					WHERE ( ${FACT_KEYS_LIST} ) NOT IN ( SELECT ${FACT_KEYS_LIST} FROM ${VIEW_DB}.${VIEW_PREFIX}${TARGET_TABLE})
					"
		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
	else
		chk_err -r 1 -m "Keys required to insert records using temporary table"
	fi
	print_msg "###################################################################################"
	print_msg ""
	set_activity_count insert
	audit_log 3 
	
}

###########################################################################################################
# Function Name : update_std_fact_from_temp_cdh
# Description   : This function updates records in standard fact target table using data in temporary table
###########################################################################################################

function update_std_fact_from_temp_cdh {

	if [[ -n $FACT_KEYS ]]
	then
		UPDATE_SQL="UPDATE tgt
                  FROM ${TARGET_DB}.$TARGET_TABLE tgt, ${TEMP_DB}.${TEMP_TABLE} src
                  SET       $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo "${UPDATE_OTHER_FIELDS_LIST} ," ;else echo "${UPDATE_OTHER_FIELDS_LIST}"; fi)
                     ${UPDATE_FACT_FIELDS_LIST}
                     , RCD_UPD_TS=$(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')
                  WHERE ${FACT_JOIN_LIST}
                "
                
		run_query -d "$TARGET_DB" -q "$UPDATE_SQL" -m "Unable to update Fact Records"
	else
		chk_err -r 1 -m "Keys required to update records using temporary table"
	fi
	
	set_activity_count update
	audit_log 3 
	
}


######################################################################################
# Function Name : delete_target_records_from_stage
# Description   : This function will delete the stage data which were inserted into target sucessfully. 
######################################################################################
function delete_target_records_from_stage {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Delecting the Records from stage table ${STAGE_TABLE}, which are inserted to target."
	if [[ -n $FACT_KEYS ]]
	then
		DELETE_SQL="DELETE FROM ${SRC_DB}.${SOURCE_TABLE}
				WHERE (${FACT_KEYS_LIST}) in (SELECT ${FACT_KEYS_LIST} FROM ${TARGET_DB}.${TARGET_TABLE})
                "
      
		run_query -d "$SRC_DB" -q "$DELETE_SQL" -m "Unable to delete records from stage"
	else
		chk_err -r 1 -m "Fact table PK required to delete from stage table"
	fi
   
   print_msg "Deleting from stage completed successfully"
   print_msg "###################################################################################"
   print_msg ""
}
