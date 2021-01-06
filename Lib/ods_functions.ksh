######################################################################################
# script : ods_functions.ksh                                                    
# Description : This library function holds the ods tables processing functions        
# Modifications                                                                  
# 8/21/2015   : Logic  : Initial Script                                                           
######################################################################################

######################################################################################
# Function Name : insert_fact_from_temp_ods
# Description   : This function loads target table, selecting data from temp table
######################################################################################

function insert_fact_from_temp_ods {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Inserting Facts into ${TARGET_DB}.${TARGET_TABLE} from ${TEMP_DB}.${TEMP_TABLE}"
	if [[ -n $FACT_IDNT_LIST ]]
	then
		INSERT_SQL="INSERT INTO ${TARGET_DB}.${TARGET_TABLE}
                  (       ${FACT_IDNT_LIST}
						  $(if [[ -n ${LOOKUP_KEYS_LIST} ]]; then echo ", ${LOOKUP_KEYS_LIST}" ;else echo "${LOOKUP_KEYS_LIST}"; fi)
					      $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
						  $(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
						, RCD_INS_TS
						, RCD_UPD_TS 
				   )                           
					SELECT ${FACT_IDNT_LIST}
						   $(if [[ -n ${LOOKUP_KEYS_LIST} ]]; then echo ", ${LOOKUP_KEYS_LIST}" ;else echo "${LOOKUP_KEYS_LIST}"; fi)
						   $(if [[ -n ${OTHER_FIELDS_LIST} ]]; then echo ", ${OTHER_FIELDS_LIST}" ;else echo "${OTHER_FIELDS_LIST}"; fi)
					       $(if [[ -n ${FACT_FIELDS_LIST} ]]; then echo ", ${FACT_FIELDS_LIST}" ;else echo "${FACT_FIELDS_LIST}"; fi)
					     , COALESCE(RCD_INS_TS, $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS')) RCD_INS_TS
					     , $(DATATYPE_CONV "'$DATETIME'" TIMESTAMP 'YYYYMMDDHHMISS') RCD_UPD_TS
					FROM ${TEMP_DB}.${TEMP_TABLE} src
					"

		run_query -d "$TARGET_DB" -q "$INSERT_SQL" -m "Unable to Insert Fact Records"
		
	else
		chk_err -r 1 -m "Identifiers required to insert records using temporary table"
	fi
	
	print_msg "###################################################################################"
	print_msg ""
	
	set_activity_count insert
	audit_log 3 
   
}


######################################################################################
# Function Name : delete_fact_using_temp_ods
# Description   : This function deletes data from target table, that exists in temp table
######################################################################################

function delete_fact_using_temp_ods {
	print_msg ""
	print_msg "###################################################################################"
	print_msg "Deleting records from ${TARGET_DB}.${TARGET_TABLE} using ${TEMP_DB}.${TEMP_TABLE}"
	
	if [[ -n $FACT_IDNT_LIST ]]
	then
		print_msg "BEFORE DELETE"
		DELETE_SQL="DELETE FROM ${TARGET_DB}.${TARGET_TABLE} 
					WHERE (${FACT_IDNT_LIST}) 
						IN ( 	SELECT ${FACT_IDNT_LIST}
								FROM ${TEMP_DB}.${TEMP_TABLE}
							)
					"

		run_query -d "$TARGET_DB" -q "$DELETE_SQL" -m "Unable to Delete Fact Records from target table"
		
	else
		chk_err -r 1 -m "Identifiers required to delete records from target table"
	fi
	
	print_msg "###################################################################################"
	print_msg ""
	
	set_activity_count delete
	audit_log 3 
}
