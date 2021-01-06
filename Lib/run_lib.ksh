. ${LIB_DIR}/utility.ksh
. ${LIB_DIR}/print_msg.ksh
. ${LIB_DIR}/chk_err.ksh
. ${LIB_DIR}/run_query.ksh
. ${LIB_DIR}/send_mail.ksh
. ${LIB_DIR}/scheduler_functions.ksh
. ${LIB_DIR}/dim_functions.ksh
. ${LIB_DIR}/fact_functions.ksh
. ${LIB_DIR}/load_functions.ksh
. ${CONTROL_SCRIPTS_DIR}/log_archive.ksh

####################################################################################################
# Function Name: set_variables                                                                     #
# Description  : This function will initiate the functions listed in it when a script is executed  #
# 9/12/2014                                                                                        #
####################################################################################################

function set_variables
{
   get_module_type ${SCRIPT_NAME}
   
   get_module_load_type ${SCRIPT_NAME}
   
   get_batch_id ${MODULE_TYPE}

   get_job_id ${MODULE_TYPE}
   
   set_audit_log_var
   
   get_bookmark ${SCRIPT_NAME} ${BATCH_ID}
   
   get_current_dt
   
   get_load_mode
   
   get_primary_currency
   
   #check_dependencies

   if [[ $SOURCE_TABLE == STG_* ]]; then
      set_loading_variable
   fi

   if [[ $TARGET_TABLE == DWH_F_* ]]; then
      set_fact_variable
   elif [[ $TARGET_TABLE == DWH_D_* ]]; then
      set_dimension_variable
   fi
   
}


function GET_TXN_CREATE_DS
{
	DATA=$1
	DATATYPE_CONV $DATA TIMESTAMP 'YYYYMMDDHHMISS'
}

################################################################################################
# Function Name: DATATYPE_CONV                                                                 #
# Description  : This function converts the data type of input.                                #
# parameter    : Data , Target datatype, Target format                                         #
# 9/12/2014                                                                                    #
################################################################################################

function DATATYPE_CONV
{
    DATA=$1
    TRG_DATATYPE=$2
    TRG_FORMAT=$3

    TRG_FORMAT_STRING=""
    if [[ "${TRG_FORMAT}" != "" ]]
    then
        TRG_FORMAT_STRING="FORMAT '$3'"
    fi

    # Generic result for DATE, TIMESTAMP, VARCHAR(n)
    RESULT="CAST($1 AS $2 ${TRG_FORMAT_STRING})"

    # Handle INTERVAL DAY TO SECOND
    if [[ "${TRG_DATATYPE}" = "INTERVAL" ]]
    then
        RESULT="($1) $3"
    fi

    echo "${RESULT}"
}









 
