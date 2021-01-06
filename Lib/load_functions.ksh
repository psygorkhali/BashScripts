#!/bin/ksh

###############################################################################################
# Script Name : load_functions.ksh                                                            #
# Description :                                                                               #
# Modifications                                                                               #
# 12/27/2012  : Logic   : Initial Script                                                      #
# 07/08/2015  : mkhanal : added session limit for fast load 
###############################################################################################

######################################################################################
# Function Name : truncate_staging_table
# Description   : This function Deletes the record from staging table
######################################################################################

function truncate_staging_table {

	# Staging table will be truncated in fastload #

    if [ ! -z $1 ] 
	then 
		SOURCE_TABLE=$1
	else
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
	
    echo "Truncate Staging Table"

    truncate_table -d "${SRC_DB}" -t "${SOURCE_TABLE}"

    print_msg "${SOURCE_TABLE} truncated successfully"
    
	
	check_err1_tbl_sql="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='ERR1_${SOURCE_TABLE#????}'"
	check_err2_tbl_sql="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='ERR2_${SOURCE_TABLE#????}'"
	
	print_msg "Checking availability of error1 table"
	is_err1_tbl=$(get_result -d "${SRC_DB}" -q "$check_err1_tbl_sql" -m "Unable to get Table layout")
	
	
	if [[ $is_err1_tbl -eq 1 ]]
	then
		err1_tbl=ERR1_${SOURCE_TABLE#????}
		drop_err1_tbl_sql="drop table ${SRC_DB}.$err1_tbl"
		print_msg "Droping $SRC_DB.$err1_tbl table"
		run_query -d "$SRC_DB" -q "$drop_err1_tbl_sql" -m "Unable to drop $SRC_DB.$err1_tbl" 	
	fi
	
	print_msg "Checking availability of error2 table"
	is_err2_tbl=$(get_result -d "${SRC_DB}" -q "$check_err2_tbl_sql" -m "Unable to get Table layout")
	if [[ $is_err2_tbl -eq 1 ]]
	then
		err2_tbl=ERR2_${SOURCE_TABLE#????}
		drop_err2_tbl_sql="drop table ${SRC_DB}.$err2_tbl"
		print_msg "Droping $SRC_DB.$err2_tbl table"
		run_query -d "$SRC_DB" -q "$drop_err2_tbl_sql" -m "Unable to drop $SRC_DB.$err2_tbl" 	
	fi
	 
	#print_msg "Retrieving DDL of staging table"
	#get_stg_table="show table ${SRC_DB}.${SOURCE_TABLE}"
	#get_stg_table_query=$(get_result -d "${SRC_DB}" -q "$get_stg_table" -m "Unable to get ${SRC_DB}.${SOURCE_TABLE} DDL query")
	
	#print_msg "STG QUERY= ${get_stg_table_query}"
	
	#recreate_stg_table="CREATE MULTISET TABLE ${SRC_DB}.${SOURCE_TABLE} ,NO FALLBACK , "${get_stg_table_query}
	#
	#drop_stg_tbl_sql="drop table ${SRC_DB}.${SOURCE_TABLE}"
	#print_msg "Dropping ${SRC_DB}.${SOURCE_TABLE} table"
	#run_query -d "$SRC_DB" -q "$drop_stg_tbl_sql" -m "Unable to drop ${SRC_DB}.${SOURCE_TABLE}" 
    #
	#print_msg "Recreating ${SRC_DB}.${SOURCE_TABLE} table"
	#run_query -d "$SRC_DB" -q "$recreate_stg_table" -m "Unable to recreate ${SRC_DB}.${SOURCE_TABLE}" 
}

########################################################################################
# Function Name : set_loading_variable
# Description   : This function holds data and bad file location in different varaibales
########################################################################################

function set_loading_variable {

    DELIMITER="|"
    SKIP_ROWS=0
	CURR_DAY_F=${CURR_DAY//-}
	
	## Adding filter for Cyclic
	
	if [[ ${MODULE_TYPE} = 'CYC' ]];then
	
			PREFIX_DATA_FILE="`(echo "${SOURCE_TABLE#STG_}" | tr [:upper:] [:lower:])`_${CURR_DAY_F}"
			FIRST_FILE=$(echo `ls ${DATA_DIR}/${PREFIX_DATA_FILE}_*.txt | head -1`)

			if [[ ${MODULE_LOAD_TYPE} = 'POSITIONAL' ]];then	
			SEQ_ID=`echo $FIRST_FILE | sed -e "s/\(.*\)_\(.*\).txt/\2/"`		
					
			DATA_FILE="${DATA_DIR}/${PREFIX_DATA_FILE}_${SEQ_ID}.txt"
			BAD_FILE="${BAD_DIR}/${SCRIPT_NAME}_${DATEKEY}_${SEQ_ID}.bad"
			REJ_FILE="${REJECT_DIR}/${SCRIPT_NAME}_${DATETIME}_${SEQ_ID}.rej"		
			
		else
			
			##else MODULE_LOAD_TYPE = 'INCREMENTAL' then concatenate all files into one and move the source files to archive
			
			print_msg "PREFIX_DATA_FILE: $PREFIX_DATA_FILE"
			
			FILE_LIST=`find $DATA_DIR -maxdepth 1 -name "${PREFIX_DATA_FILE}_*" -print`
			#print_msg "File List: $FILE_LIST"
			
			DATETIME_NOW=$(date +"%H%M%S")

			for filename in $FILE_LIST
			do
				cat  ${filename} >> "${DATA_DIR}/${PREFIX_DATA_FILE}_all_${DATETIME_NOW}.txt"
				DATA=$(basename ${DATA_FILE})
				
				print_msg "Moving file ${filename} to ${ARCHIVE_DIR}"
				mv -f ${filename} ${ARCHIVE_DIR}/${DATA}
			done
			
			print_msg "Concatenated files to ${PREFIX_DATA_FILE}_all_${DATETIME_NOW}.txt"
			
			DATA_FILE="${DATA_DIR}/${PREFIX_DATA_FILE}_all_${DATETIME_NOW}.txt"
			BAD_FILE="${BAD_DIR}/${SCRIPT_NAME}_${DATEKEY}_all_${DATETIME_NOW}.bad"
			REJ_FILE="${REJECT_DIR}/${SCRIPT_NAME}_${DATETIME}.rej"	
		fi
		
	else
		DATA_FILE="${DATA_DIR}/$(echo "${SOURCE_TABLE#STG_}" | tr [:upper:] [:lower:])_${CURR_DAY_F}_${BATCH_ID}.txt"	
		BAD_FILE="${BAD_DIR}/${SCRIPT_NAME}_${DATEKEY}_${BATCH_ID}.bad"
		REJ_FILE="${REJECT_DIR}/${SCRIPT_NAME}_${DATETIME}.rej"
	fi
}

########################################################################################
# Function Name : get_err1_count
# Description   : This function is defined in utility.ksh. The function can return value
#                 after fastloadlog_parse function is called
########################################################################################

function get_err1_count {

    # This function is defined in utility.ksh. The function can return value after fastloadlog_parse function is called.
    get_fastload_err1_count
    
}

############################################################################################
# Function Name : load_data
# Description   : This function loads source table after truncating all the existing records
#                 Also Checks in Error table for bad data during fast load, if data exist 
#                 then create a backup file for missed data and DROP error table
############################################################################################

function load_data {

#######OPTIONAL_PARAMETERS######
	if [ ! -z $1 ] 
	then 
		SOURCE_TABLE=$1
	else
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
	if [ ! -z $2 ] 
	then 
		DATA_FILE=$2
	else
		DATA_FILE=${DATA_FILE}
	fi
#######END OPTIONAL_PARAMETERS######
  if [[ ${SEED_FLAG} = 0 ]];then
	print_msg "Checking file integrity $(basename ${DATA_FILE})"
	check_file_integrity
  fi

    print_msg "Loading ${SOURCE_TABLE}"
    
    ((START_FROM=SKIP_ROWS+1))
    
    define_file_layout
    
    logfile_last_line_num=$(cat $LOG_FILE | wc -l )
    ((logfile_last_line_num++))
    
fastload << EOF 2>&1 >> $LOG_FILE
	.sessions $FASTLOAD_MAX_SESSIONS
	.LOGON ${HOST}/${USER},${PASSWORD}
	DATABASE ${SRC_DB};          
	DELETE FROM ${SRC_DB}.${SOURCE_TABLE} ALL;
	RECORD ${START_FROM};
	SET RECORD VARTEXT "${DELIMITER}";
	DEFINE
	${FILE_LAYOUT}
	FILE = ${DATA_FILE} ;
	BEGIN LOADING ${SRC_DB}.${SOURCE_TABLE} ERRORFILES ${SRC_DB}.ERR1_${SOURCE_TABLE#????}, ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
	INSERT INTO ${SRC_DB}.${SOURCE_TABLE}.*;
	END LOADING;
	.LOGOFF
EOF
ret_code=$?

    fastloadlog=$(tail -n+${logfile_last_line_num} $LOG_FILE)
    fastloadlog_parse

    chk_err -r $ret_code -m "Failed to load staging table ${SOURCE_TABLE}: $(get_fastload_error)"
	
	set_activity_count fastload
	audit_log 1
	
	##Check in Error table for bad data during fast load, if data exist then create a backup file for missed data and DROP error table
	
	export err1_count=$(get_err1_count)
    print_msg "Err1 table count: $err1_count"
    if [[ $err1_count > 0 ]]; then
        extract_bad_data
		#get_reject_columns
    fi
	
    print_msg "${SOURCE_TABLE} loaded successfully"
	

}

################################################################################################
# Function Name : load_reject_data
# Description   : Function loads the data rejected before current batch (not specifically the 
#				  immediate before) when loading in temp table 
################################################################################################

function load_reject_data {

 print_msg "Loading rejected data into stage table SOURCE_TABLE"
 REJECT_PROCESS_TABLE=DWH_C_REJ_TBL_PROCESS
 REJ_TABLE="REJ_${TARGET_TABLE#DWH_}"
 check_reject_process_status="SELECT count(*) FROM ${TARGET_DB}.${REJECT_PROCESS_TABLE} where REJECT_TABLE_NAME='${REJ_TABLE}' and PROCESS_READY_STATUS='Y'"
 process_ready=$(get_result -d "${TARGET_DB}" -q "$check_reject_process_status" -m "Unable to get Script Key")
 
 
 if [[ $process_ready -eq 1 ]]
      then
      
      column_list="SELECT COLUMNNAME FROM DBC.COLUMNS WHERE DATABASENAME = '${SRC_DB}' AND TABLENAME = '${SOURCE_TABLE}'"
      COL_TMP=$(get_result -d "$SRC_DB" -q "$column_list" -m "Unable to get Column List of ${SOURCE_TABLE}")
      
      COL="${COL_TMP}"
      
      
      CHECK=1
   
    for field in $COL
    do
   
      if [[ $CHECK -eq 1 ]]
      then
         COL_LIST="$field"
         CHECK=0
      else
         COL_LIST="${COL_LIST}, $field"
      fi
      
   done


 # Inserting data into stage table from source table
   INSERT_REJ_SQL="INSERT INTO ${SRC_DB}.${SOURCE_TABLE}
  			(${COL_LIST})
  			 SELECT 
  				${COL_LIST} 
  			 FROM 
  				${SRC_DB}.${REJ_TABLE}"
  		
    run_query -d "$SRC_DB" -q "$INSERT_REJ_SQL" -m "Unable to Insert Records into rejection table" 		
    
    REJ_UPD_SQL="UPDATE ${TARGET_DB}.${REJECT_PROCESS_TABLE}
    			SET  PROCESS_READY_STATUS='C'
    			WHERE REJECT_TABLE_NAME='${REJ_TABLE}'
    			AND PROCESS_READY_STATUS='Y' "
    			
    run_query -d "$TARGET_DB" -q "$REJ_UPD_SQL" -m "Unable to update Records into rejection table" 	
    
    TRUNCATE_SQL="DELETE ${SRC_DB}.${REJ_TABLE} all"
    
    run_query -d "$SRC_DB" -q "$TRUNCATE_SQL" -m "Unable to truncate reject Records " 	

fi

}
############################################################################################
# Function Name : load_vdate
# Description   : This function is used only once to load vdate.
#                 This function loads source table after truncating all the existing records
#                 Also Checks in Error table for bad data during fast load, if data exist 
#                 then create a backup file for missed data and DROP error table.
############################################################################################
function load_vdate {

    print_msg "Loading ${SOURCE_TABLE}"
    DATA_FILE="${DATA_DIR}/d_vdate.txt" # Due to dependency for date field (needed even before date is extracted) in the text file we are not using date field here
	echo  ${DATA_FILE}
	
	print_msg "Checking file integrity $(basename ${DATA_FILE})"
	check_file_integrity
	
    ((START_FROM=SKIP_ROWS+1))
    
    define_file_layout
    
    logfile_last_line_num=$(cat $LOG_FILE | wc -l )
    ((logfile_last_line_num++))
    
fastload << EOF 2>&1 >> $LOG_FILE
	.sessions $FASTLOAD_MAX_SESSIONS
	.LOGON ${HOST}/${USER},${PASSWORD}
	DATABASE ${SRC_DB};          
	DELETE FROM ${SRC_DB}.${SOURCE_TABLE} ALL;
	RECORD ${START_FROM};
	SET RECORD VARTEXT "${DELIMITER}";
	DEFINE
	${FILE_LAYOUT}
	FILE = ${DATA_FILE} ;
	BEGIN LOADING ${SRC_DB}.${SOURCE_TABLE} ERRORFILES ${SRC_DB}.ERR1_${SOURCE_TABLE#????}, ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
	INSERT INTO ${SRC_DB}.${SOURCE_TABLE}.*;
	END LOADING;
	.LOGOFF
EOF
ret_code=$?

    fastloadlog=$(tail -n+${logfile_last_line_num} $LOG_FILE)
    fastloadlog_parse

    chk_err -r $ret_code -m "Failed to load staging table ${SOURCE_TABLE}: $(get_fastload_error)"
	

	
	##Check in Error table for bad data during fast load, if data exist then create a backup file for missed data and DROP error table
	
	export err1_count=$(get_err1_count)
    print_msg "Err1 table count: $err1_count"
    if [[ $err1_count > 0 ]]; then
        extract_bad_data
		#get_reject_columns
    fi
	
    print_msg "${SOURCE_TABLE} loaded successfully"

}


############################################################################################
# Function Name : mload_data
# Description   : This function loads source table using mload function
############################################################################################


function mload_data {
	print_msg "Mloading ${SOURCE_TABLE}"
	
	if [ ! -z $1 ] 
	then 
		MLOAD_FILE=$1
	else
		MLOAD_FILE=${DATA_FILE}
	fi
    
   ((START_FROM=SKIP_ROWS+1))
   
    define_file_layout_mload
    
    mload_last_line_num=$(cat $LOG_FILE | wc -l )
    ((mload_last_line_num++))
	

mload << EOF 2>&1 >> $LOG_FILE
  .Logtable Logtable_${SOURCE_TABLE#????};          
  .LOGON ${HOST}/${USER},${PASSWORD};
  .Begin Import Mload              
     tables                        
        ${SRC_DB}.${SOURCE_TABLE};     
 .Layout Transaction;             
    ${FILE_LAYOUT_MLOAD}    
  .DML Label Inserts;              
  Insert into ${SRC_DB}.${SOURCE_TABLE}.*;           
  .Import Infile ${MLOAD_FILE}        
	FORMAT VARTEXT '|'
     Layout Transaction                      
  Apply Inserts;                   
  .End Mload;                      
  .Logoff;         
EOF

 mloadlog=$(tail -n+${mload_last_line_num} $LOG_FILE)
 mload_log_parse

mload_rejects


#MLOAD_RELEASE="RELEASE MLOAD ${SRC_DB}.${SOURCE_TABLE}"
#run_query -d "$SRC_DB" -q "$MLOAD_RELEASE" -m "Unable to release mload for ${SRC_DB}.${SOURCE_TABLE}" 	
}




#################################################################################################
# Function Name : mload_data_fixed
# Description   : This function loads source table using mload function from fixed length file
#################################################################################################

function mload_data_fixed {

while getopts "s:d:f:q:" arg
   do
      case $arg in
       s ) SOURCE_TABLE=$OPTARG;;

       d )  MLOAD_FILE=$OPTARG;;

       f )  FILE_LAYOUT_MLOAD_FIXED=$OPTARG;;
	   
	   q )  MLOAD_QUERY=$OPTARG;;

       \? ) chk_err -r 1 -m 'load_data_fixed usage: -s source_table -d data_file -f file_layout_mload_fixed'

	   esac
done

#######OPTIONAL_PARAMETERS######
   
   if [[ "$SOURCE_TABLE" == "" ]]
   then
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
   if [[ "$DATA_FILE" == "" ]]
   then
		MLOAD_FILE=${DATA_FILE}
	fi

   if [[ "$FILE_LAYOUT_MLOAD_FIXED" == "" ]]
   then
		define_file_layout_mload_fixed
	fi
	
	if [[ "$MLOAD_QUERY" == "" ]]
   then
		MLOAD_QUERY="INSERT INTO ${SRC_DB}.${SOURCE_TABLE}.*"
	fi

#######END OPTIONAL_PARAMETERS######
	
   if [[ "$SOURCE_TABLE" == "" ]]
   then
       chk_err -r 1 -m "No SOURCE_TABLE passed to load_data_fixed function"
   fi

   if [[ "$DATA_FILE" == "" ]]
   then
       chk_err -r 1 -m "No DATA_FILE passed to load_data_fixed function"
   fi
 
   if [[ "$FILE_LAYOUT_MLOAD_FIXED" == "" ]]
   then
       chk_err -r 1 -m "No FILE_LAYOUT_MLOAD_FIXED passed to load_data_fixed function"
   fi 
 
	print_msg "Mloading ${SOURCE_TABLE}"

	print_msg "New data file : $DATA_FILE"
	print_msg "New mload_query : ${MLOAD_QUERY}"
	
   ((START_FROM=SKIP_ROWS+1))
    
	mload_last_line_num=$(cat $LOG_FILE | wc -l )
    ((mload_last_line_num++))
	
	print_msg "layout for mload is ${FILE_LAYOUT_MLOAD_FIXED}"
	
drop_table -d ${SRC_DB} -t UV_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t ET_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t WT_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t LOGTABLE_${SOURCE_TABLE#????};
	
bteq << release!

.LOGON ${HOST}/${USER},${PASSWORD};

release mload ${SRC_DB}.${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.UV_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.ET_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.WT_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.LOGTABLE_${SOURCE_TABLE#????};

.LOGOFF;        
.QUIT 0;

release!
	
	
mload << EOF 2>&1 >> $LOG_FILE
  .Logtable ${SRC_DB}.Logtable_${SOURCE_TABLE#????};          
  .LOGON ${HOST}/${USER},${PASSWORD};
  .Begin Import Mload              
     tables                        
        ${SRC_DB}.${SOURCE_TABLE};     
 .Layout Transaction;             
    ${FILE_LAYOUT_MLOAD_FIXED}   
  .DML Label Inserts;              
 ${MLOAD_QUERY};    
  .Import Infile ${MLOAD_FILE}        
	FORMAT TEXT
     Layout Transaction                      
  Apply Inserts;                   
  .End Mload;                      
  .Logoff;         
EOF
ret_code=$?


 mloadlog=$(tail -n+${mload_last_line_num} $LOG_FILE)
 mloadlog_parse

if [[ ret_code -gt 0 ]] then

bteq << release!

.LOGON ${HOST}/${USER},${PASSWORD};

release mload ${SRC_DB}.${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.UV_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.ET_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.WT_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.LOGTABLE_${SOURCE_TABLE#????};

.LOGOFF;        
.QUIT 0;

release!
	
fi

 chk_err -r $ret_code -m "Failed to load staging table ${SOURCE_TABLE}"

	mload_rejects
	set_activity_count mload
	audit_log 1
	
    print_msg "${SOURCE_TABLE} loaded successfully"
	
}

#################################################################################################
# Function Name : mload_data_fixed
# Description   : This function loads source table using mload function from fixed length file
#################################################################################################

function mload_data_fixed_payrollitm {

while getopts "s:d:f:q:" arg
   do
      case $arg in
       s ) SOURCE_TABLE=$OPTARG;;

       d )  MLOAD_FILE=$OPTARG;;

       f )  FILE_LAYOUT_MLOAD_FIXED=$OPTARG;;
	   
	   q )  MLOAD_QUERY=$OPTARG;;

       \? ) chk_err -r 1 -m 'load_data_fixed usage: -s source_table -d data_file -f file_layout_mload_fixed'

	   esac
done

#######OPTIONAL_PARAMETERS######
   
   if [[ "$SOURCE_TABLE" == "" ]]
   then
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
   if [[ "$DATA_FILE" == "" ]]
   then
		MLOAD_FILE=${DATA_FILE}
	fi

   if [[ "$FILE_LAYOUT_MLOAD_FIXED" == "" ]]
   then
		define_file_layout_mload_fixed
	fi
	
	if [[ "$MLOAD_QUERY" == "" ]]
   then
		MLOAD_QUERY="INSERT INTO ${SRC_DB}.${SOURCE_TABLE}.*"
	fi

#######END OPTIONAL_PARAMETERS######
	
   if [[ "$SOURCE_TABLE" == "" ]]
   then
       chk_err -r 1 -m "No SOURCE_TABLE passed to load_data_fixed function"
   fi

   if [[ "$DATA_FILE" == "" ]]
   then
       chk_err -r 1 -m "No DATA_FILE passed to load_data_fixed function"
   fi
 
   if [[ "$FILE_LAYOUT_MLOAD_FIXED" == "" ]]
   then
       chk_err -r 1 -m "No FILE_LAYOUT_MLOAD_FIXED passed to load_data_fixed function"
   fi 
 
	print_msg "Mloading ${SOURCE_TABLE}"

	print_msg "New data file : $DATA_FILE"
	print_msg "New mload_query : ${MLOAD_QUERY}"
	
   ((START_FROM=SKIP_ROWS+1))
    
	mload_last_line_num=$(cat $LOG_FILE | wc -l )
    ((mload_last_line_num++))
	
	print_msg "layout for mload is ${FILE_LAYOUT_MLOAD_FIXED}"
	
drop_table -d ${SRC_DB} -t UV_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t ET_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t WT_${SOURCE_TABLE};
drop_table -d ${SRC_DB} -t LOGTABLE_${SOURCE_TABLE#????};
	
bteq << release!

.LOGON ${HOST}/${USER},${PASSWORD};

release mload ${SRC_DB}.${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.UV_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.ET_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.WT_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.LOGTABLE_${SOURCE_TABLE#????};

.LOGOFF;        
.QUIT 0;

release!
	
	
mload << EOF 2>&1 >> $LOG_FILE
  .Logtable ${SRC_DB}.Logtable_${SOURCE_TABLE#????};          
  .LOGON ${HOST}/${USER},${PASSWORD};
  .Begin Import Mload              
     tables                        
        ${SRC_DB}.${SOURCE_TABLE};     
 .Layout Transaction;             
    ${FILE_LAYOUT_MLOAD_FIXED}   
  .DML Label Inserts;              
 ${MLOAD_QUERY};    
  .Import Infile ${MLOAD_FILE}        
	FORMAT TEXT
     Layout Transaction                      
  Apply Inserts
 WHERE IN_TRXN_TYPE ='J'
   AND IN_caRec_Type_J1 = '1';  
  .End Mload;                      
  .Logoff;         
EOF
ret_code=$?


 mloadlog=$(tail -n+${mload_last_line_num} $LOG_FILE)
 mloadlog_parse

if [[ ret_code -gt 0 ]] then

bteq << release!

.LOGON ${HOST}/${USER},${PASSWORD};

release mload ${SRC_DB}.${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.UV_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.ET_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.WT_${SOURCE_TABLE};
DROP TABLE ${SRC_DB}.LOGTABLE_${SOURCE_TABLE#????};

.LOGOFF;        
.QUIT 0;

release!
	
fi

 chk_err -r $ret_code -m "Failed to load staging table ${SOURCE_TABLE}"

	mload_rejects
	set_activity_count mload
	audit_log 1
	
    print_msg "${SOURCE_TABLE} loaded successfully"
	
}


#function mload_data_fixed {
#	print_msg "Mloading ${SOURCE_TABLE}"
#	
#	if [ ! -z $1 ] 
#	then 
#		MLOAD_FILE=$1
#	else
#		MLOAD_FILE=${DATA_FILE}
#	fi
#    
#   ((START_FROM=SKIP_ROWS+1))
#   
#    define_file_layout_mload_fixed
#    
#	mload_last_line_num=$(cat $LOG_FILE | wc -l )
#    ((mload_last_line_num++))
#	
#	print_msg "layout for mload is ${FILE_LAYOUT_MLOAD_FIXED}"
#	
#	
#	
#drop_table -d ${SRC_DB} -t UV_${SOURCE_TABLE};
#drop_table -d ${SRC_DB} -t ET_${SOURCE_TABLE};
#drop_table -d ${SRC_DB} -t WT_${SOURCE_TABLE};
#drop_table -d ${SRC_DB} -t LOGTABLE_${SOURCE_TABLE#????};
#	
#bteq << release!
#
#.LOGON ${HOST}/${USER},${PASSWORD};
#
#release mload ${SRC_DB}.${SOURCE_TABLE};
#
#
#.LOGOFF;        
#.QUIT 0;
#
#release!
#	
#	
#mload << EOF 2>&1 >> $LOG_FILE
#  .Logtable ${SRC_DB}.Logtable_${SOURCE_TABLE#????};          
#  .LOGON ${HOST}/${USER},${PASSWORD};
#  .Begin Import Mload              
#     tables                        
#        ${SRC_DB}.${SOURCE_TABLE};     
# .Layout Transaction;             
#    ${FILE_LAYOUT_MLOAD_FIXED}    
#  .DML Label Inserts;              
#  Insert into ${SRC_DB}.${SOURCE_TABLE}.*;           
#  .Import Infile ${MLOAD_FILE}        
#	FORMAT TEXT
#     Layout Transaction                      
#  Apply Inserts;                   
#  .End Mload;                      
#  .Logoff;         
#EOF
#
# mloadlog=$(tail -n+${mload_last_line_num} $LOG_FILE)
# mload_log_parse
#
#
#mload_rejects
#	
#}
#
#


#######################################################################################################################
# Function Name : mload_rejects
# Description   : This function loads mload rejected data in reject table
#######################################################################################################################

function mload_rejects {
	check_work_table="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='WT_${SOURCE_TABLE}'"
	check_et_table="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='ET_${SOURCE_TABLE}'"
	check_uv_table="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='UV_${SOURCE_TABLE}'"
	check_log_table="SELECT count(*) FROM dbc.tables WHERE databasename='${SRC_DB}' and tablename='Logtable_${SOURCE_TABLE#????}'"
	
	
	print_msg "Checking availability of work table for mload"
	is_work_table=$(get_result -d "${SRC_DB}" -q "$check_work_table" -m "Unable to get Table layout")
	if [[ $is_work_table -eq 1 ]]
	then
		drop_wk_tbl_sql="drop table ${SRC_DB}.WT_${SOURCE_TABLE}"
		print_msg "Dropping work table ${SRC_DB}.WT_${SOURCE_TABLE}"
		run_query -d "$SRC_DB" -q "$drop_wk_tbl_sql" -m "Unable to drop $SRC_DB.WT_${SOURCE_TABLE}" 	
	fi


	print_msg "Checking availability of et table for mload"
	is_et_table=$(get_result -d "${SRC_DB}" -q "$check_et_table" -m "Unable to get Table layout")
	if [[ $is_et_table -eq 1 ]]
	then
		drop_et_tbl_sql="drop table ${SRC_DB}.ET_${SOURCE_TABLE}"
		print_msg "Dropping et table ${SRC_DB}.ET_${SOURCE_TABLE}"
		run_query -d "$SRC_DB" -q "$drop_et_tbl_sql" -m "Unable to drop $SRC_DB.ET_${SOURCE_TABLE}" 	
	fi	
	
	print_msg "Checking availability of uv table for mload"
	is_uv_table=$(get_result -d "${SRC_DB}" -q "${check_uv_table}" -m "Unable to get Table layout")

	
	if [[ $is_uv_table -eq 1 ]]
	then
		
		SELECT_UV_LAYOUT="SELECT
								CASE WHEN ColumnId = (min(ColumnId) over (order by ColumnId)) THEN ' ' ELSE ',' END
								|| TRIM(ColumnName)
							FROM DBC.COLUMNS WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')"
							
		UV_LAYOUT_MLOAD=$(get_result -d "${SRC_DB}" -q "$SELECT_UV_LAYOUT" -m "Unable to get Table layout")
	
		print_msg "Creating reject data file for mload reject: ${REJ_FILE}"
		#print_msg "SELECT ${UV_LAYOUT_MLOAD} FROM ${SRC_DB}.UV_${SOURCE_TABLE};"
		 bteq << EOF >> /dev/null
		.LOGON ${HOST}/${USER},${PASSWORD}
		DATABASE ${SRC_DB};
		.set width 64000;
		.set recordmode on ;
		.set titledashes off ;
		.Set Format off
		.Set Null ''
		.set separator '|'
		.Set ERROROUT STDERR
		.Export REPORT File=${REJ_FILE}
		SELECT ${UV_LAYOUT_MLOAD} FROM ${SRC_DB}.UV_${SOURCE_TABLE};
		.EXPORT RESET;
		--DROP TABLE ${SRC_DB}.ERR1_${SOURCE_TABLE#????};
		--DROP TABLE ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
		.LOGOFF;
		.QUIT;
		.EXIT
EOF

		FILESIZE=$(stat -c%s "${REJ_FILE}")
		if [[ $FILESIZE == 0 ]]; then
			rm $REJ_FILE
		else        
			# Get number of column in staging table. It is required to process the bad file.
			GET_NUMCOLUMNS_SQL="SELECT trim(count(*)) FROM dbc.columns where databasename='${SRC_DB}' and tablename='${SOURCE_TABLE}'"
			numColumns=$(get_result -d "$SRC_DB" -q "$GET_NUMCOLUMNS_SQL" -m "Unable to get number of columns")
			#print_msg "${LIB_DIR}/postprocess_baddata $REJ_FILE ${REJ_FILE}.tmp "${DELIMITER}" ${numColumns}"
			# ${LIB_DIR}/postprocess_baddata $REJ_FILE ${REJ_FILE}.tmp "${DELIMITER}" ${numColumns}
			#rm $REJ_FILE
			#mv $REJ_FILE.tmp $REJ_FILE
			#rm $REJ_FILE.tmp
		fi
		
		
		drop_uv_tbl_sql="drop table ${SRC_DB}.UV_${SOURCE_TABLE}"
		print_msg "Dropping et table ${SRC_DB}.UV_${SOURCE_TABLE}"
		run_query -d "$SRC_DB" -q "$drop_uv_tbl_sql" -m "Unable to drop $SRC_DB.UV_${SOURCE_TABLE}" 	
	fi	
	
	
	print_msg "Checking availability of log table for mload"
	is_log_table=$(get_result -d "${SRC_DB}" -q "${check_log_table}" -m "Unable to get Table layout")
	
	
	if [[ ${is_log_table} -eq 1 ]]
	then
		print_msg "Dropping log table"
		droping_logtable="drop table ${SRC_DB}.Logtable_${SOURCE_TABLE#????}"
		run_query -d "$SRC_DB" -q "$droping_logtable" -m "Unable to drop Logtable_${SOURCE_TABLE#????}" 	
	fi

}

########################################################################################
# Function Name : load_dummy
# Description   : This function is used to release fast load lock in tables
########################################################################################

function load_dummy {
	
    print_msg "Releasing Fast Load lock on ${SOURCE_TABLE}"
    
  if [ ! -z $1 ] 
	then 
		SOURCE_TABLE=$1
	else
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
	if [ ! -z $2 ] 
	then 
		DATA_FILE=$2
	else
		DATA_FILE=${DATA_FILE}
	fi
	
	
#######END OPTIONAL_PARAMETERS######
    #print_msg "Checking file integrity $(basename ${DATA_FILE})"
    #check_file_integrity

    print_msg "Loading ${SOURCE_TABLE}"
    
    ((START_FROM=SKIP_ROWS+1))
    
	#define_file_layout
	
	if [ ! -z $3 ] 
	then 
		FILE_LAYOUT_FIXED=${3}
	else
		define_file_layout_fastload
	fi
	    #define_file_layout_fastload
    
    #logfile_last_line_num=$(cat $LOG_FILE | wc -l )
    #((logfile_last_line_num++))

    #SET RECORD UNFORMATTED;
	#DEFINE
	#${FILE_LAYOUT_FIXED}
	#newline (CHAR(1))  
	
fastload << EOF 2>&1 >> $LOG_FILE
	.sessions $FASTLOAD_MAX_SESSIONS
	.LOGON ${HOST}/${USER},${PASSWORD}
	DATABASE ${SRC_DB};
	BEGIN LOADING ${SRC_DB}.${SOURCE_TABLE} ERRORFILES ${SRC_DB}.ERR1_${SOURCE_TABLE#????}, ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
	END LOADING;
	.LOGOFF
EOF
ret_code=$? 

    #fastloadlog=$(tail -n+${logfile_last_line_num} $LOG_FILE)
    #fastloadlog_parse
    #
    #chk_err -r $ret_code -m "Failed to unlock staging table ${SOURCE_TABLE}: $(get_fastload_error)"
    #
    #print_msg "${SOURCE_TABLE} dummy loaded successfully"

}


#######################################################################################################################
# Function Name : load_data_fixed
# Description   : This function loads source table from fixed length file after truncating all the existing records
#                 Also Checks in Error table for bad data during fast load, if data exist 
#                 then create a backup file for missed data and DROP error table
#######################################################################################################################

function load_data_fixed {

#######OPTIONAL_PARAMETERS######
	if [ ! -z $1 ] 
	then 
		SOURCE_TABLE=$1
	else
		SOURCE_TABLE=${SOURCE_TABLE}
	fi
	
	if [ ! -z $2 ] 
	then 
		DATA_FILE=$2
	else
		DATA_FILE=${DATA_FILE}
	fi
	
	
#######END OPTIONAL_PARAMETERS######
    #print_msg "Checking file integrity $(basename ${DATA_FILE})"
    #check_file_integrity

    print_msg "Loading ${SOURCE_TABLE}"
    
    ((START_FROM=SKIP_ROWS+1))
    
	#define_file_layout
	
	if [ ! -z $3 ] 
	then 
		FILE_LAYOUT_FIXED=${3}
	else
		define_file_layout_fastload
	fi
	
    #define_file_layout_fastload
    
    logfile_last_line_num=$(cat $LOG_FILE | wc -l )
    ((logfile_last_line_num++))

#  
fastload << EOF 2>&1 >> $LOG_FILE
	.sessions $FASTLOAD_MAX_SESSIONS
	.LOGON ${HOST}/${USER},${PASSWORD}
	DATABASE ${SRC_DB};          
	DELETE FROM ${SRC_DB}.${SOURCE_TABLE} ALL;
	SET RECORD UNFORMATTED;
	DEFINE
	${FILE_LAYOUT_FIXED}
	newline (CHAR(1))  

	FILE = ${DATA_FILE} ;
	BEGIN LOADING ${SRC_DB}.${SOURCE_TABLE} ERRORFILES ${SRC_DB}.ERR1_${SOURCE_TABLE#????}, ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
	INSERT INTO ${SRC_DB}.${SOURCE_TABLE}.*;
	END LOADING;
	.LOGOFF
EOF
ret_code_saved=$?

	fastloadlog=$(tail -n+${logfile_last_line_num} $LOG_FILE)
	
	print_msg "$fastloadlog"
    fastloadlog_parse
	
	if [[ $ret_code_saved -gt 0 ]] 
	then
	  print_msg "Error in fast load need to release lock"
	  #print_msg "Calling code to release lock"
	  load_dummy ${SOURCE_TABLE} ${DATA_FILE}
	fi
    
    chk_err -r $ret_code_saved -m "Failed to load staging table ${SOURCE_TABLE}: $(get_fastload_error)"
		
	set_activity_count fastload
	audit_log 1
	
	###Check in Error table for bad data during fast load, if data exist then create a backup file for missed data and DROP error table
	#
	#export err1_count=$(get_err1_count)
    #echo "Err1 table count: $err1_count"
    #if [[ $err1_count > 0 ]]; then
    #    extract_bad_data
	#	#get_reject_columns
    #fi
	
    print_msg "${SOURCE_TABLE} loaded successfully"

}




########################################################################################
# Function Name : extract_bad_data
# Description   : This function extracts rejected data from Fast Load error table 1 into 
#				  flat-file and converts the Hex data into original source format
########################################################################################

function extract_bad_data {
print_msg "Creating reject data file: ${REJ_FILE}"
		 bteq << EOF >> /dev/null
		.LOGON ${HOST}/${USER},${PASSWORD}
		DATABASE ${SRC_DB};
		.set width 64000;
		.set recordmode on ;
		.set titledashes off ;
		.Set Format off
		.Set Null ''
		.set separator '|'
		.Set ERROROUT STDERR
		.Export DATA File=${REJ_FILE}
		SELECT DataParcel FROM ${SRC_DB}.ERR1_${SOURCE_TABLE#????};
		.EXPORT RESET;
		--DROP TABLE ${SRC_DB}.ERR1_${SOURCE_TABLE#????};
		--DROP TABLE ${SRC_DB}.ERR2_${SOURCE_TABLE#????};
		.LOGOFF;
		.QUIT;
		.EXIT
EOF

    ## Structure the bad data file.
	FILESIZE=$(stat -c%s "${REJ_FILE}")
	if [[ ${FILESIZE} == 0 ]]; then
		rm $REJ_FILE;
	else        
        # Get number of column in staging table. It is required to process the bad file.
        GET_NUMCOLUMNS_SQL="SELECT trim(count(*)) FROM dbc.columns where databasename='${SRC_DB}' and tablename='${SOURCE_TABLE}'"
        numColumns=$(get_result -d "$SRC_DB" -q "$GET_NUMCOLUMNS_SQL" -m "Unable to get number of columns")
		print_msg "REJECT FILE ${REJ_FILE} created"
		print_msg "${LIB_DIR}/postprocess_baddata $REJ_FILE ${REJ_FILE}.tmp ${DELIMITER} ${numColumns}"
        (cd ${BIN_DIR}; ${LIB_DIR}/postprocess_baddata $REJ_FILE ${REJ_FILE}.tmp "${DELIMITER}" ${numColumns})
		rm -f $REJ_FILE
		mv -f $REJ_FILE.tmp $REJ_FILE
		rm -f $REJ_FILE.tmp
	fi

	NOTIFICATION_TIME=$(date +"%Y-%m-%d %H:%M:%S")
	export MAIL_SUBJECT="Warning in script $SCRIPT_NAME.ksh"
	echo "Warning in script $SCRIPT_NAME.ksh executed at $NOTIFICATION_TIME" > $EMAILMESSAGE
	echo "Warning: $err1_count rows rejected when loading $TARGET_TABLE from $DATA_FILE" >>$EMAILMESSAGE
	#echo "getting error row count = $err1_count"
	if [[ $err1_count -gt 0 ]]
	then
		reject_mail
	fi
	
	if [[ ${STAGE_REJECT_LIMIT} -gt 0 && ${err_count} -ge ${STAGE_REJECT_LIMIT} ]]
	then
	   print_msg "Maximum Rejection Allowed  While Stage Load Reached. Exiting..."
	   exit 1
	fi
}

########################################################################################
# Function Name : get_reject_columns
# Description   : This function extracts count of rejected data from Fast Load error table 1 and column name for which data is rejected 
#  				  and writes information in log file.
########################################################################################

function get_reject_columns {

		echo "Retrieving column for which data is rejected."
		print_msg "-----------------------------------------------------------------"
		REJECTED_COLUMN_NUMBER_SQL="SELECT COUNT(*) FROM ${SRC_DB}.ERR1_${SOURCE_TABLE#????}
					"		 
					
		REJECTED_NUMBER_OF_COLUMNS=$(get_result -d "$SRC_DB" -q "$REJECTED_COLUMN_NUMBER_SQL" -m "Unable to get number of columns rejected for ${SCRIPT_NAME}")			
		
		print_msg "$REJECTED_NUMBER_OF_COLUMNS rows are rejected during fastload."
		print_msg "-----------------------------------------------------------------"
		
		declare -a rejectedColumns
		
		REJECTED_COLUMNS_SQL="SELECT DISTINCT ERRORFIELDNAME FROM ${SRC_DB}.ERR1_${SOURCE_TABLE#????}
					"
		rejectedColumns=$(get_result -d "$SRC_DB" -q "$REJECTED_COLUMNS_SQL" -m "Unable to get number of columns rejected for ${SCRIPT_NAME}")			
				
		print_msg "Data for $rejectedColumns[@] are rejected."		
		print_msg "-----------------------------------------------------------------"
					
		for column in $rejectedColumns
		do 
			COLUMN_NUMBER_SQL="SELECT COUNT(*) FROM ${SRC_DB}.ERR1_${SOURCE_TABLE#????} WHERE ERRORFIELDNAME = $column
					"		 
			NUMBER_OF_COLUMNS=$(get_result -d "$SRC_DB" -q "$COLUMN_NUMBER_SQL" -m "Unable to get number of columns rejected for ${column}")			
		
			print_msg "$NUMBER_OF_COLUMNS rows are rejected during fastload for $column."
			print_msg "-----------------------------------------------------------------"
		done
		
		
}
########################################################################################
# Function Name : define_file_layout
# Description   : This function describes the flat file layout.
########################################################################################

function define_file_layout {
    
    SELECT_QUERY="SELECT
    CASE WHEN ColumnId = (min(ColumnId) over (order by ColumnId)) THEN ' ' ELSE ',' END
    || ColumnName
    || '(VARCHAR('
    || CASE WHEN ColumnType = 'I1' THEN '4' /* ByteInt */
            WHEN ColumnType = 'I2' THEN '6' /* SmallInt */
            WHEN ColumnType = 'I ' THEN '11' /* Integer */
            WHEN ColumnType = 'I8' THEN '20' /* BigInt */
            WHEN ColumnType = 'D ' THEN TRIM(DecimalTotalDigits + 1) /* Decimal */
            WHEN ColumnType in ('CV','CF','BV','BF') THEN TRIM(CAST(ColumnLength AS INTEGER)) /* VARCHAR,CHAR,VARBYTEBYTE */
            WHEN ColumnType = 'DA' THEN '10' /* DATE */
            WHEN ColumnType = 'AT' THEN TRIM(9 + DecimalFractionalDigits) /* Time */
            WHEN ColumnType = 'TS' THEN TRIM(20 + DecimalFractionalDigits) /* TIMESTAMP */
       ELSE '255' END
    || '))'
    FROM DBC.COLUMNS WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
    "
    
    FILE_LAYOUT=$(get_result -d "${SRC_DB}" -q "$SELECT_QUERY" -m "Unable to get Table layout")
}


########################################################################################
# Function Name : define_file_layout_fastload
# Description   : This function describes the flat file layout for fastload.
########################################################################################

function define_file_layout_fastload {
	 SELECT_QUERY_LAYOUT="SELECT
    CASE WHEN ColumnId = (min(ColumnId) over (order by ColumnId)) THEN ' ' ELSE ',' END
    || ColumnName
    || '(CHAR('
    || CASE WHEN ColumnType = 'I1' THEN '4' /* ByteInt */
            WHEN ColumnType = 'I2' THEN '6' /* SmallInt */
            WHEN ColumnType = 'I ' THEN '11' /* Integer */
            WHEN ColumnType = 'I8' THEN '20' /* BigInt */
            WHEN ColumnType = 'D ' THEN TRIM(DecimalTotalDigits + 1) /* Decimal */
            WHEN ColumnType in ('CV','CF','BV','BF') THEN TRIM(CAST(ColumnLength AS INTEGER)) /* VARCHAR,CHAR,VARBYTEBYTE */
            WHEN ColumnType = 'DA' THEN '8' /* DATE */
            WHEN ColumnType = 'AT' THEN TRIM(9 + DecimalFractionalDigits) /* Time */
            WHEN ColumnType = 'TS' THEN TRIM(20 + DecimalFractionalDigits) /* TIMESTAMP */
       ELSE '255' END
    || ')'
    || CASE WHEN   ColumnType = 'DA' THEN ', NULLIF = ''00000000''' ELSE '' END
    || ')'     
    FROM DBC.COLUMNS WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
    "
    
    FILE_LAYOUT_FIXED=$(get_result -d "${SRC_DB}" -q "$SELECT_QUERY_LAYOUT" -m "Unable to get Table layout")
 
}




########################################################################################
# Function Name : define_file_layout_mload
# Description   : This function describes the flat file layout for mload.
########################################################################################

function define_file_layout_mload {

#SELECT_QUERY="SELECT
#    CASE WHEN ColumnId = (min(ColumnId) over (order by ColumnId)) THEN ' ' ELSE ',' END
#    || ColumnName
#    || '(VARCHAR('
#    || CASE WHEN ColumnType = 'I1' THEN '4' /* ByteInt */
#            WHEN ColumnType = 'I2' THEN '6' /* SmallInt */
#            WHEN ColumnType = 'I ' THEN '11' /* Integer */
#            WHEN ColumnType = 'I8' THEN '20' /* BigInt */
#            WHEN ColumnType = 'D ' THEN TRIM(DecimalTotalDigits + 1) /* Decimal */
#            WHEN ColumnType in ('CV','CF','BV','BF') THEN TRIM(CAST(ColumnLength AS INTEGER)) /* VARCHAR,CHAR,VARBYTEBYTE */
#            WHEN ColumnType = 'DA' THEN '10' /* DATE */
#            WHEN ColumnType = 'AT' THEN TRIM(9 + DecimalFractionalDigits) /* Time */
#            WHEN ColumnType = 'TS' THEN TRIM(20 + DecimalFractionalDigits) /* TIMESTAMP */
#       ELSE '255' END
#    || '))'
#    FROM DBC.COLUMNS WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
#    "
	
	 SELECT_QUERY_MLOAD="SELECT
		'.FIELD '||TRIM(COLUMNNAME)||' * '||TRIM(COLUMNTYPE)||TRIM(COLUMNNUM)||';'
		FROM (
		  SELECT DATABASENAME, TABLENAME, COLUMNNAME,
		  CASE 
			WHEN COLUMNTYPE='CF' THEN 'CHAR'
			WHEN COLUMNTYPE='CV' THEN 'VARCHAR'
			WHEN COLUMNTYPE='D'  THEN 'DECIMAL' 
			WHEN COLUMNTYPE='TS' THEN 'TIMESTAMP'      
			WHEN COLUMNTYPE='I'  THEN 'INTEGER'
			WHEN COLUMNTYPE='I2' THEN 'SMALLINT'
			WHEN COLUMNTYPE='DA' THEN 'DATE'  
		  END AS COLUMNTYPE,
		  CASE 
			WHEN COLUMNTYPE='CF' THEN '('||TRIM(COLUMNLENGTH)||')'
			WHEN COLUMNTYPE='CV' THEN '('||TRIM(COLUMNLENGTH)||')'
			WHEN COLUMNTYPE='D'  THEN '('||(TRIM(DECIMALTOTALDIGITS)||','||TRIM(DECIMALFRACTIONALDIGITS))||')'
			WHEN COLUMNTYPE='TS' THEN '('||TRIM(COLUMNLENGTH)||')'     
			WHEN COLUMNTYPE='I'  THEN '11'
			WHEN COLUMNTYPE='I2' THEN '6'
			WHEN COLUMNTYPE='DA' THEN '8'
		  END AS COLUMNNUM
		  FROM DBC.COLUMNS
		 WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
		) TBL"

FILE_LAYOUT_MLOAD=$(get_result -d "${SRC_DB}" -q "$SELECT_QUERY_MLOAD" -m "Unable to get Table layout")
}




########################################################################################
# Function Name : define_file_layout_mload_fixed
# Description   : This function describes the flat fixed length file layout for mload.
########################################################################################

function define_file_layout_mload_fixed {
	 SELECT_QUERY_MLOAD_FIXED="SELECT
		'.FIELD '||TRIM(COLUMNNAME)||' * '||TRIM(COLUMNTYPE)||TRIM(COLUMNNUM)||');'
		FROM (
		  SELECT DATABASENAME, TABLENAME, COLUMNNAME,
		  'CHAR(' AS COLUMNTYPE,
		  CASE WHEN ColumnType = 'I1' THEN '4' /* ByteInt */
				WHEN ColumnType = 'I2' THEN '6' /* SmallInt */
				WHEN ColumnType = 'I ' THEN '11' /* Integer */
				WHEN ColumnType = 'I8' THEN '20' /* BigInt */
				WHEN ColumnType = 'D ' THEN TRIM(DecimalTotalDigits + 1) /* Decimal */
				WHEN ColumnType in ('CV','CF','BV','BF') THEN TRIM(CAST(ColumnLength AS INTEGER)) /* VARCHAR,CHAR,VARBYTEBYTE */
				WHEN ColumnType = 'DA' THEN '8' /* DATE */
				WHEN ColumnType = 'AT' THEN TRIM(9 + DecimalFractionalDigits) /* Time */
				WHEN ColumnType = 'TS' THEN TRIM(20 + DecimalFractionalDigits) /* TIMESTAMP */
				ELSE '255' 
			END AS COLUMNNUM
		  FROM DBC.COLUMNS
		 WHERE UPPER(TableName) = UPPER('${SOURCE_TABLE}') AND UPPER(DatabaseName) = UPPER('${SRC_DB}')
		) TBL"

FILE_LAYOUT_MLOAD_FIXED=$(get_result -d "${SRC_DB}" -q "$SELECT_QUERY_MLOAD_FIXED" -m "Unable to get Table layout")
}

function load_data_fixed_arg {

while getopts "s:d:f:q:" arg
   do
      case $arg in
       s )  FSOURCE_TABLE=$OPTARG;;

       d )  FLOAD_FILE=$OPTARG;;

       f )  FILE_LAYOUT_FLOAD_FIXED=$OPTARG;;
	   
	   q )  FLOAD_QUERY=$OPTARG;;

       \? ) chk_err -r 1 -m 'load_data_fixed usage: -s source_table -d data_file -f file_layout_mload_fixed'

	   esac
   done
     
		print_msg "query: ${FLOAD_QUERY}"
   #######OPTIONAL_PARAMETERS######
   
   if [[ "$FSOURCE_TABLE" == "" ]]
   then
		FSOURCE_TABLE=${SOURCE_TABLE}
	fi
	
   if [[ "$DATA_FILE" == "" ]]
   then
		FLOAD_FILE=${DATA_FILE}
	fi

   if [[ "$FILE_LAYOUT_FLOAD_FIXED" == "" ]]
   then
		define_file_layout_fastload
	fi
	
	if [[ "$FLOAD_QUERY" == "" ]]
   then
		FLOAD_QUERY="INSERT INTO ${SRC_DB}.${FSOURCE_TABLE}.*"
	fi

print_msg "New source table is $SOURCE_TABLE"
#######END OPTIONAL_PARAMETERS######
	
   if [[ "$FSOURCE_TABLE" == "" ]]
   then
       chk_err -r 1 -m "No SOURCE_TABLE passed to load_data_fixed function"
   fi

   if [[ "$DATA_FILE" == "" ]]
   then
       chk_err -r 1 -m "No DATA_FILE passed to load_data_fixed function"
   fi
 
   if [[ "$FILE_LAYOUT_FLOAD_FIXED" == "" ]]
   then
       chk_err -r 1 -m "No FILE_LAYOUT_FLOAD_FIXED passed to load_data_fixed function"
   fi 
	
	#######END OPTIONAL_PARAMETERS######
    
    #check_file_integrity

    print_msg "Loading ${FSOURCE_TABLE}"
    
    ((START_FROM=SKIP_ROWS+1))
 
    logfile_last_line_num=$(cat $LOG_FILE | wc -l )
    ((logfile_last_line_num++))

#  
fastload << EOF 2>&1 >> $LOG_FILE
	.sessions $FASTLOAD_MAX_SESSIONS
	.LOGON ${HOST}/${USER},${PASSWORD}
	DATABASE ${SRC_DB};          
	DELETE FROM ${SRC_DB}.${FSOURCE_TABLE} ALL;
	SET RECORD UNFORMATTED;
	DEFINE
	${FILE_LAYOUT_FLOAD_FIXED}
	newline (CHAR(1))  

	FILE = ${DATA_FILE} ;
	BEGIN LOADING ${SRC_DB}.${FSOURCE_TABLE} ERRORFILES ${SRC_DB}.ERR1_${FSOURCE_TABLE#????}, ${SRC_DB}.ERR2_${FSOURCE_TABLE#????};
	${FLOAD_QUERY};
	END LOADING;
	.LOGOFF
EOF
ret_code=$?

    fastloadlog=$(tail -n+${logfile_last_line_num} $LOG_FILE)
    fastloadlog_parse

    chk_err -r $ret_code -m "Failed to load staging table ${FSOURCE_TABLE}: $(get_fastload_error)"
	
	set_activity_count fastload
	audit_log 1
	
	###Check in Error table for bad data during fast load, if data exist then create a backup file for missed data and DROP error table
	#
	#export err1_count=$(get_err1_count)
    #echo "Err1 table count: $err1_count"
    #if [[ $err1_count > 0 ]]; then
    #    extract_bad_data
	#	#get_reject_columns
    #fi
	
    print_msg "${FSOURCE_TABLE} loaded successfully"

}

