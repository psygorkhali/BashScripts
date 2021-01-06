#!/bin/ksh

################################################################################################
# Script Name : run_query.ksh                                                                  #
# Description : This library functions defines run_query function which takes                  #
#               a database, query and err_msg as arguments. It runs the query                  #
#               against the database and exits with the err_msg in case query fails.           #
# Modifications                                                                                #
# 9/11/2014   : Logic  : Initial Script                                                       #
################################################################################################



function run_query {

   ret_code=0
   query_err_msg="Running Query failed"
   query_db=""
   query_command=""
   query_file=""

   while getopts "d:q:m:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       q )  query_command=$OPTARG;;

       m )  query_err_msg=$OPTARG;;

       \? ) chk_err -r 1 -m 'run_query usage: -d database -q query -m err_msg'

      esac
   done

   if [[ $DEBUG_FLAG == 1 ]]
   then
   # echo "query_db : $query_db" >> $LOG_FILE
    echo "Start:************************"  >> $LOG_FILE
    echo "query_command : $query_command" >> $LOG_FILE
    echo "END:************************"  >> $LOG_FILE
	echo "" >> $LOG_FILE
   # echo "query_err_msg : $query_err_msg" >> $LOG_FILE
   fi
   #return 1;

 
   if [[ "$query_db" == "" ]]
   then
       #print_msg "No database passed to run_query function"
       chk_err -r 1 -m "No database passed to run_query function"
   fi

   if [[ "$query_command" == "" ]]
   then
       #print_msg "No Query passed to run_query function"
       chk_err -r 1 -m "No Query passed to run_query function"
   fi

   if [[ "$query_err_msg" == "" ]]
   then
      query_err_msg="Running direct query failed"
   fi

   logfile_last_line_num=$(cat $LOG_FILE | wc -l )
   ((logfile_last_line_num++))

   #bteq << EOF &1 >> $LOG_FILE &2>> $ERROR_FILE
  # 
  #bteq << EOF $LOG_FILE 2>> $ERROR_FILE
 # bteq << EOF 2>&1 >> $LOG_FILE
 #bteq << EOF 2>&2 >> $ERROR_FILE

 
 bteq_log=$(bteq << EOF
   .LOGON ${HOST}/${USER},${PASSWORD}
   DATABASE $query_db;
   .SET MAXERROR 7
   .Set ERROROUT STDERR
   .SET ECHOREQ OFF
   .IF ERRORCODE <> 0 THEN .RUN FILE = display_last_SQL.run
    $query_command;
	.IF ERRORCODE <> 0 THEN .RUN FILE = display_last_SQL.run
   .EXIT
EOF)
ret_code=$?


   #set_bteqlog "$(tail -n+${logfile_last_line_num} $LOG_FILE)"
   set_bteqlog "${bteq_log}"
   
   if [[ $ret_code != "0" ]]; then
        query_err_msg="${query_err_msg}:$(get_last_error_for_sql)"
		print_err "The query for above error:   "
		print_err "${query_command}"
   fi
 
   chk_err -r $ret_code -m "$query_err_msg"
}
################################################################################################
# Function Name : check_temp_reject
# Description   : Function checks the data rejected when loading in temp table and updated the 
#				  DWH_C_REJ_TBL_PROCESS and create /update the respective reject table is Stage
#                 Database as well 
################################################################################################	

function check_temp_reject {

	print_msg "Checking temp reject records"
	REJECT_PROCESS_TABLE=DWH_C_REJ_TBL_PROCESS
	if [[ -n ${REJ_IDNT_LIST} ]]
	then
	     TEMP_COUNT_SQL="SELECT count(*) FROM (SELECT DISTINCT ${REJ_IDNT_LIST} FROM ${TEMP_DB}.${TEMP_TABLE})TMP"
	    temp_count=$(get_result -d "$TEMP_DB" -q "$TEMP_COUNT_SQL" -m "Unable to get count in ${TEMP_TABLE}")
	    
	    STAGE_COUNT_SQL="SELECT count(*) FROM (SELECT DISTINCT ${REJ_IDNT_LIST} FROM ${SRC_DB}.${SOURCE_TABLE})TMP"
	    stage_count=$(get_result -d "${SRC_DB}" -q "${STAGE_COUNT_SQL}" -m "Unable to get count in ${SOURCE_TABLE}")
	
	elif [[ -n ${DIM_IDNT_LIST} || -n ${FACT_IDNT_LIST} ]]
	then
	    TEMP_COUNT_SQL="SELECT count(*) FROM (SELECT DISTINCT ${DIM_IDNT_LIST}${FACT_IDNT_LIST} FROM ${TEMP_DB}.${TEMP_TABLE})TMP"
	    temp_count=$(get_result -d "$TEMP_DB" -q "$TEMP_COUNT_SQL" -m "Unable to get count in ${TEMP_TABLE}")
	    
	    STAGE_COUNT_SQL="SELECT count(*) FROM (SELECT DISTINCT ${DIM_IDNT_LIST}${FACT_IDNT_LIST} FROM ${SRC_DB}.${SOURCE_TABLE})TMP"
	    stage_count=$(get_result -d "${SRC_DB}" -q "${STAGE_COUNT_SQL}" -m "Unable to get count in ${SOURCE_TABLE}")
	else
	    TEMP_COUNT_SQL="SELECT count(*) FROM ${TEMP_DB}.${TEMP_TABLE}"
	    temp_count=$(get_result -d "$TEMP_DB" -q "$TEMP_COUNT_SQL" -m "Unable to get count in ${TEMP_TABLE}")
	    
	    STAGE_COUNT_SQL="SELECT count(*) FROM ${SRC_DB}.${SOURCE_TABLE}"
	    stage_count=$(get_result -d "${SRC_DB}" -q "${STAGE_COUNT_SQL}" -m "Unable to get count in ${SOURCE_TABLE}")
	fi
	
	if [[ ${temp_count} != ${stage_count} ]]
	then
		REJ_TABLE="REJ_${TARGET_TABLE#DWH_}"
		
		check_table_sql="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='${REJ_TABLE}'"
		
		table_exist=$(get_result -d "$SRC_DB" -q "$check_table_sql" -m "Unable to get count of ${REJ_TABLE}")
   

		if [[ $table_exist -eq 1 ]]
		then
					
			print_msg "Checking feasibility for truncating Reject table ${REJ_TABLE}"
			COMPLETE_SQL="SELECT PROCESS_READY_STATUS
							FROM
							(
							SELECT F.*, ROW_NUMBER() OVER (PARTITION BY REJECT_TABLE_NAME ORDER BY BATCH_ID DESC ,JOB_ID DESC,REJ_TIMESTAMP DESC) RNK from  ${VIEW_DB}.${VIEW_PREFIX}${REJECT_PROCESS_TABLE} f
							WHERE REJECT_TABLE_NAME= '${REJ_TABLE}'
							) AA
							WHERE RNK=1"
							
			reject_data_exist=$(get_result -d "${TARGET_DB}" -q "${COMPLETE_SQL}" -m "Unable to reject process status")
			
			
			if [[ ${reject_data_exist} = 'C' ]]
			then
				print_msg "Truncating rejection table ${TEMP_DB}.${REJ_TABLE}"
				truncate_table -d ${SRC_DB} -t ${REJ_TABLE}
			fi
			
			print_msg "Loading reject data in reject table : ${SRC_DB}.${REJ_TABLE}"
			
		else
			GET_SRC_SQL="SELECT
									TRIM(TBL.COLUMNNAME)||'   '||TRIM(TBL.COLUMNTYPE)||TRIM(TBL.COLUMNNUM)||','
									FROM (
									SELECT DATABASENAME, TABLENAME, COLUMNNAME,
									CASE
									WHEN COLUMNTYPE='CF' THEN 'CHAR'
									WHEN COLUMNTYPE='CV' THEN 'VARCHAR'
									WHEN COLUMNTYPE='D'  THEN 'DECIMAL'
									WHEN COLUMNTYPE='TS' THEN 'TIMESTAMP'
									WHEN COLUMNTYPE='I'  THEN 'INTEGER'
									WHEN COLUMNTYPE='I8'  THEN 'BIGINT'
									WHEN COLUMNTYPE='I2' THEN 'SMALLINT'
									WHEN COLUMNTYPE='DA' THEN 'DATE'
									END AS COLUMNTYPE,
									CASE
										WHEN COLUMNTYPE='CF' THEN '('||TRIM(CAST (COLUMNLENGTH AS INTEGER))||')'
										WHEN COLUMNTYPE='CV' THEN '('||TRIM(CAST (COLUMNLENGTH AS INTEGER))||')'
										WHEN COLUMNTYPE='D'  THEN '('||(TRIM(DECIMALTOTALDIGITS)||','||TRIM(DECIMALFRACTIONALDIGITS))||')'
										WHEN COLUMNTYPE='TS' THEN '('||TRIM(CAST (DECIMALFRACTIONALDIGITS AS INTEGER))||')'
										WHEN COLUMNTYPE='I'  THEN ''
										WHEN COLUMNTYPE='I8'  THEN ''
										WHEN COLUMNTYPE='I2' THEN ''
										WHEN COLUMNTYPE='DA' THEN ''
									END AS COLUMNNUM,
									COLUMNID AS COLUMNID
									FROM DBC.COLUMNS
									WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
									) TBL 
									ORDER BY COLUMNID
							"
							
				COLUMN_LIST=$(get_result -d "$SRC_DB" -q "$GET_SRC_SQL" -m "Unable to retrieve column list of stage table" )
				
				CREATE_REJ_SQL="create table ${SRC_DB}.${REJ_TABLE}
								(${COLUMN_LIST}
								 BUSINESS_DT DATE FORMAT 'YYYY-MM-DD'
								,REJ_TIMESTAMP TIMESTAMP(6)
								,BATCH_ID INTEGER
								,JOB_ID INTEGER
								)
				"
				
				run_query -d "$SRC_DB" -q "$CREATE_REJ_SQL" -m "Unable to create rejection table" 
				
		fi		
	
		GET_SRC_COLUMNS="SELECT
						       TRIM(COLUMNNAME)||','
						 FROM (
						       SELECT DATABASENAME, TABLENAME, COLUMNNAME
						       FROM DBC.COLUMNS
						       WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
								) TBL"
	
		STG_COLUMN_LIST=$(get_result -d "$SRC_DB" -q "$GET_SRC_COLUMNS" -m "Unable to get columns of stage table" )
				
		REJ_SQL="INSERT INTO ${SRC_DB}.${REJ_TABLE} (${STG_COLUMN_LIST} BUSINESS_DT,REJ_TIMESTAMP,BATCH_ID,JOB_ID)
						SELECT 
							${STG_COLUMN_LIST}
                            $(DATATYPE_CONV "'$CURR_DAY'" DATE 'YYYY-MM-DD')						
							,CURRENT_TIMESTAMP	REJ_TIMESTAMP
							,${BATCH_ID} 		BATCH_ID
							,${JOB_ID}   		JOB_ID
						FROM 
							${SRC_DB}.${SOURCE_TABLE} src
						WHERE (${REJ_IDNT_LIST}  $(if [[ -z ${REJ_IDNT_LIST} ]]; then echo "${DIM_IDNT_LIST} ${FACT_IDNT_LIST}"; fi)) NOT IN 
							( SELECT 

								${REJ_IDNT_LIST} $(if [[ -z ${REJ_IDNT_LIST} ]]; then echo "${DIM_IDNT_LIST} ${FACT_IDNT_LIST}"; fi)
							FROM 
								${TEMP_DB}.${TEMP_TABLE}
							)
						"
						
		run_query -d "$SRC_DB" -q "$REJ_SQL" -m "Unable to Insert Records into rejection table" 
	
	
		
		print_msg "Recording log of reject data in ${REJECT_PROCESS_TABLE} table"
		
		REJ_PROCESS_SQL="INSERT INTO ${TARGET_DB}.${REJECT_PROCESS_TABLE}
		                                                                (BATCH_ID
		                                                                ,JOB_ID
		                                                                ,REJECT_TABLE_NAME
		                                                                ,PROCESS_READY_STATUS
		                                                                ,REJ_TIMESTAMP
		                                                                )
		                                                                values
		                                                                ( ${BATCH_ID}
		                                                                ,${JOB_ID}
		                                                                ,'${REJ_TABLE}'
		                                                                ,'N'
		                                                                ,current_timestamp )"
	
	
		run_query -d "$TARGET_DB" -q "$REJ_PROCESS_SQL" -m "Unable to Insert Records into rejection table" 
	
		REJ_COUNT="SELECT TRIM(COUNT(*)) FROM ${SRC_DB}.${REJ_TABLE} WHERE JOB_ID=${JOB_ID} AND BUSINESS_DT = DATE '${CURR_DAY}' "
		REJ_COUNT_REJ=$(get_result -d "$TEMP_DB" -q "$REJ_COUNT" -m "Unable to get rejected row count")
		print_msg "REJ_COUNT = $REJ_COUNT_REJ"
		
		NOTIFICATION_TIME=$(date +"%Y-%m-%d %H:%M:%S")
		export MAIL_SUBJECT="Warning in script $SCRIPT_NAME.ksh"
		print_msg "Warning in script $SCRIPT_NAME.ksh executed at $NOTIFICATION_TIME" > $EMAILMESSAGE
		print_msg "Warning: $REJ_COUNT_REJ rows rejected in $TARGET_TABLE" >>$EMAILMESSAGE
		if [[ $REJ_COUNT_REJ -gt 0 ]]
		then
			reject_mail
		fi
		
		if [[ ${TEMP_REJECT_LIMIT} -gt 0 && ${REJ_COUNT_REJ} -ge ${TEMP_REJECT_LIMIT} ]]
		then
		   print_msg "Maximum Rejection Allowed  While Temp Load Reached. Exiting..."
		   exit 1
		fi
		set_audit_log_var
		
		NO_OF_ROW_REJECTED=${REJ_COUNT_REJ}
		
		audit_log 2
	else
		print_msg "NO data rejected"
	fi
}


###############################################################################################
# Function Name : get_result
# Description   : This library functions fetch the result from the database. It takes query,
#                 database name as a parameter and queries it into the database.
###############################################################################################

function get_result {

   ret_code=0
   query_err_msg="Selecting records failed"
   query_db=""
   query_command=""
   query_file=""

   while getopts "d:q:m:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       q )  query_command=$OPTARG;;

       m )  query_err_msg=$OPTARG;;

       \? ) chk_err -r 1 -m 'run_query usage: -d database -q query -m err_msg'

      esac
   done

    if [[ $DEBUG_FLAG == 1 ]]
    then
    # echo "query_db : $query_db" >> $LOG_FILE
     echo "Start:************************"  >> $LOG_FILE
     echo "query_command : $query_command" >> $LOG_FILE
     echo "END:************************"  >> $LOG_FILE
	 echo "" >> $LOG_FILE
    # echo "query_err_msg : $query_err_msg" >> $LOG_FILE
    fi
    #return 1;


   if [[ "$query_db" == "" ]]
   then
       #print_msg "No database passed to get_result function"
       chk_err -r 1 -m "No database passed to get_result function"
   fi

   if [[ "$query_command" == "" ]]
   then
       #print_msg "No Query passed to get_result function"
       chk_err -r 1 -m "No Query passed to get_result function"
   fi

   if [[ "$query_err_msg" == "" ]]
   then
      query_err_msg="Running direct query failed"
   fi

   tmp_filename=$(mktemp -p ${TMP_DIR} tmp_${SCRIPT_NAME}.XXXXXXXXXX)
   if [[ "$?" != 0 ]]
   then
       chk_err -r 1 -m "get_result function: Cannot create temp file"
   fi

   # bteq << EOF &1 >> $LOG_FILE &2>>$ERROR_FILE
  # bteq << EOF 2>&1 >> $LOG_FILE
  #bteq << EOF 2>&2 >> $ERROR_FILE
  bteq << EOF >> /dev/null
   .LOGON ${HOST}/${USER},${PASSWORD}
   DATABASE $query_db;
   .SET MAXERROR 7
   .SET TITLEDASHES OFF
   .SET FORMAT OFF
   .SET SEPARATOR '|'
   .EXPORT REPORT FILE='${tmp_filename}'
   .SET WIDTH 65531
   .SET ECHOREQ OFF
   .Set ERROROUT STDERR
   .IF ERRORCODE <> 0 THEN .RUN FILE = display_last_SQL.run
    $query_command;
	.IF ERRORCODE <> 0 THEN .RUN FILE = display_last_SQL.run
   .EXPORT RESET
   .EXIT
EOF
ret_code=$?
	
	if [[ $ret_code != 0 ]]
   then
	 print_err "$query_command"
   fi
   
    chk_err -r $ret_code -m "$query_err_msg"

    tail -n+2 ${tmp_filename}

    rm ${tmp_filename}
}



#####################################################################################################
# Function Name : truncate_table
# Description   : This library functions truncates whole table data, the table name
#                 is passed as a parameter.
#####################################################################################################

function truncate_table {
	
   ret_code=0
   query_err_msg="Running Direct Query failed"
   query_db=""
   query_command=""
   table_to_truncate=""

   while getopts "d:t:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       t ) table_to_truncate=$OPTARG;;

       \? ) chk_err -r 1 -m 'truncate_table usage: -d database -t table'
      esac
   done
print_msg "Truncating table $table_to_truncate"
   if [[ "$query_db" == "" ]]
   then
       #print_msg "No database passed to truncate_table function"
       chk_err -r 1 -m "No database passed to truncate_table function"
   fi

   if [[ "$table_to_truncate" == "" ]]
   then
       #print_msg "No table passed to truncate_table function"
       chk_err -r 1 -m "No table passed to truncate_table function"
   else
       query_command="DELETE FROM ${query_db}.$table_to_truncate ALL"
   fi

   run_query -d $query_db -q "$query_command" -m "Failed to truncate $table_to_truncate in $query_db"


}

#####################################################################################################
# Function Name : drop_table
# Description   : This library functions drop the table.
# Parameter     : Query statement and table name
#####################################################################################################

function drop_table {
   query_db=""
   table_to_drop=""
   while getopts "d:t:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       t ) table_to_drop=$OPTARG;;

       \? ) chk_err -r 1 -m 'drop_table usage: -d database -t table'
      esac
   done

   print_msg "Dropping table $table_to_drop"
   if [[ "$query_db" == "" ]]
   then
       chk_err -r 1 -m "No database passed to drop_table function"
   fi

   if [[ "$table_to_drop" == "" ]]
   then
       chk_err -r 1 -m "No table passed to drop_table function"
   else

       check_table=$(get_result -d "${SRC_DB}" -q "SELECT 1 FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='${table_to_drop}'" -m "Unable to get Table layout")
	  
       if [[ $check_table == 1 ]]
       then
          run_query -d $query_db -q "drop table $table_to_drop" -m "Failed to drop $table_to_drop"
       else
          print_msg "WARNING: $table_to_drop does not exist in $query_db"
       fi

   fi

}

function run_fexp {

   ret_code=0
   query_err_msg="Fast Export failed"
   query_db=""
   query_command=""
   query_file=""
   query_output_dir=$EXTRACTS_DIR
   query_output=""
   logtb=LOGTABLE

   while getopts "d:q:m:o:" arg
   do
      case $arg in
       d ) query_db=$OPTARG;;

       q )  query_command=$OPTARG;;

       m )  query_err_msg=$OPTARG;;

       o )  query_output=$OPTARG;;

       \? ) chk_err -r 1 -m 'run_query usage: -d database -q query -m err_msg -o output_file'

      esac
   done

   if [[ $DEBUG_FLAG == 1 ]]
   then
   # echo "query_db : $query_db" >> $LOG_FILE
    echo "Start:************************"  >> $LOG_FILE
    echo "query_command : $query_command" >> $LOG_FILE
    echo "END:************************"  >> $LOG_FILE
        echo "" >> $LOG_FILE
   # echo "query_err_msg : $query_err_msg" >> $LOG_FILE
   fi
   #return 1;


   if [[ "$query_db" == "" ]]
   then
       #print_msg "No database passed to run_query function"
       chk_err -r 1 -m "No database passed to run_query function"
   fi

   if [[ "$query_command" == "" ]]
   then
       #print_msg "No Query passed to run_query function"
       chk_err -r 1 -m "No Query passed to run_query function"
   fi

   if [[ "$query_err_msg" == "" ]]
   then
      query_err_msg="Running direct query failed"
   fi

   if [[ "$query_output" == "" ]]
   then
      query_output="Output file not specified"
   fi

   logfile_last_line_num=$(cat $LOG_FILE | wc -l )
   ((logfile_last_line_num++))


 fexp << EOF >> $LOG_FILE
   .LOGTABLE $SRC_DB.$logtb;
   .LOGON ${HOST}/${USER},${PASSWORD};
     DATABASE $query_db;

   .BEGIN EXPORT SESSIONS 4;
   .EXPORT OUTFILE $query_output_dir$query_output FORMAT TEXT MODE RECORD;
    $query_command
   .END EXPORT;
   .LOGOFF;
   .QUIT;
EOF
ret_code=$?
}
