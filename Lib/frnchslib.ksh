###########################################################################
#                                                                          
#   Script Name: frnchslib.ksh
#
#   Description: library of franchise functions for shell processing.
#
#   Functions available:
#   NAME                                            Description
#   ------------------                              -----------------------
#   frnchs_get_base_upload_date_range               Gets list of weeks for recently loaded stage data
#   frnchs_reset_earliest_activity_dte              Clears earliest activity_dte from control table 
#   frnchs_control_initial_log_engry_for_run        Sets/removes initial log entry for a run
#   frnchs_check_skus                               Validates the skus from a source table and logs errors to the error log
#   frnchs_check_id                                 Validates the franchise id from a source table and logs errors to the error log
#   frnchs_check_express_shops                      Validates the Express shop from a source table and logs errors to the error log
#   frnchs_check_frnchs_shops                       Validates the Franchise shop from a source table and logs errors to the error log
#   frnchs_set_period_date                          Calculates and sets the period end date based on the transaction date
#   frnchs_combine_short_sku                        Appends short_sku_chk_digit to short_sku when they are separate fields
#   frnchs_check_date_type                          Makes sure a date is what it's expected to be
#   frnchs_refresh_all_stats                        Refresh statistics on teradata
#   frnchs_refresh_tmp_stats                        Refresh teradata statistics on tmp database
#   frnchs_refresh_data_stats                       Refresh teradata statistics on data database
#   frnchs_refresh_arc_stats                        Refresh teradata statistics on arc database
#   frnchs_prep_franchise_files                     Prepares files from franchisee's for unwrap processing
#   frnchs_unwrap_franchise_file                    Unwraps franchisee's files
#                                                                          
#
#   AUDIT TRAIL
#	Original script frnchslib.ksh modified and develby C.Douthitt and NRichardson  	
#   =======================
#   Date        Person         Description	
#   --------    ------------   ------------------------------------------
#   20140225    C. Douthitt    Added function frnchs_combine_short_sku
#   20131002    C. Douthitt    Add functions for franchise table multiload
#   20130131    C. Douthitt    Added functions Tables_Are_Compatible
#   20140806    NRichardson    Added logic for Edcon
#
#   Modified Under Yomari
#   20150122    Yomari     Converted function frnchs_get_base_upload_date_range
###########################################################################


###########################################################################
#                                                                          
#	Function Name: 	frnchs_get_base_upload_date_range
#
#	Description:    Creates a file for weeks of activity that have been upl
#                 from a franchise feed
#
#
# Parameters:     stage table, date field name on stage table
#
# Example:        frnchs_get_base_date_range DWH_F_FR_INV_ILW_HST_B wk_end_dte
#
# Output:         file containing a list of week end dates
#
#
###########################################################################

export SCRIPT_NAME=$(basename ${0%%.ksh})

. ${ETLHOME}/etc/ENV.cfg
. ${LIB_DIR}/run_lib.ksh


 set_variables



#This function finds the range of week end date 

frnchs_get_base_upload_date_range( ) 
{
  FUNCNAME=frnchs_get_base_upload_date_range
  frnchs_get_base_upload_date_range_stg_tbl=$1
  frnchs_get_base_upload_stg_date_field=$2
  frnchs_get_base_upload_period=$3
   
if [[ -z ${frnchs_get_base_upload_date_range_stg_tbl} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}                                           "
  echo "  Missing stage table passed                                                        "
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ -z ${frnchs_get_base_upload_stg_date_field} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}                                           "
  echo "  Missing stage date field passed                                                   "
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
 
if [[ -z ${frnchs_get_base_upload_period} ]] then
  frnchs_get_base_upload_period=w
fi
   

####################### Determine the earliest Date to run for###############



    START_DATE_SEQ="SELECT cast( (min(r.CYC_END_DT)  (format 'YYYY-MM-DD')) as char(10)) 
					FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_CTRL_LOG c,
						${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_VLD_CYC_DT_LU r
					WHERE c.EARLIEST_ACTIVITY_DT <= r.CYC_END_DT
						AND r.${frnchs_get_base_upload_period}_ind    = 'Y'
						AND c.TARGET_TABLE_NAME = '${frnchs_get_base_upload_date_range_stg_tbl}'"

	START_DATE=$(get_result -d "$TARGET_DB" -q "$START_DATE_SEQ" -m "Unable to get START DATE VALUE")   
    print_msg "Date begin ${START_DATE} loaded sucessfully"
   
   
   
	END_DATE_SEQ="SELECT cast( (min(CYC_END_DT)  (format 'YYYY-MM-DD')) as char(10)) 
		FROM ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_VLD_CYC_DT_LU
		WHERE ${frnchs_get_base_upload_period}_IND = 'Y'
	 AND CYC_END_DT >= coalesce((select max(${frnchs_get_base_upload_stg_date_field}) FROM ${VIEW_DB}.${VIEW_PREFIX}${frnchs_get_base_upload_date_range_stg_tbl}),date)";  

      ##AND CYC_END_DT >= (select max(${frnchs_get_base_upload_stg_date_field}) FROM ${VIEW_DB}.${VIEW_PREFIX}${frnchs_get_base_upload_date_range_stg_tbl})";  
   
	
    END_DATE=$(get_result -d "$TARGET_DB" -q "$END_DATE_SEQ" -m "Unable to get START DATE VALUE")   
    print_msg "Date end ${END_DATE} loaded sucessfully"
   
   
   
   #start_dte=$(<${DIR_TEMP}/${frnchs_get_base_upload_date_range_stg_tbl}_start) 
   
   if [[ ${START_DATE} == "?" ]] then
      echo "No new source data available for ${frnchs_get_base_upload_date_range_stg_tbl} table, processing will be bypassed"
      exit 0
    fi
    
   # end_dte=$(<${DIR_TEMP}/${frnchs_get_base_upload_date_range_stg_tbl}_end)
   
   if [[ ${END_DATE} == "?" ]] then
      echo "  Error in frnchslib function ${FUNCNAME}"
      echo "  No max calendar date determined for table ${frnchs_get_base_upload_date_range_stg_tbl}"
      exit 1
    fi
   rm -f ${TMP_DIR}/${frnchs_get_base_upload_date_range_stg_tbl}_range
   
	bteq << !get_date_range
    .set width 255
	     
  .logon ${HOST}/${USER}, ${PASSWORD}
	DATABASE ${TARGET_DB}; 	

    .format off
	
	
     
     /*
       Build the full range of Weekly Dates to run for
     */
    .export report file = ${TMP_DIR}/${frnchs_get_base_upload_date_range_stg_tbl}_range
   SELECT CYC_END_DT  (title '', format 'yyyy-mm-dd')
		FROM  ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_VLD_CYC_DT_LU
		WHERE ${frnchs_get_base_upload_period}_ind = 'Y'
       AND CYC_END_DT BETWEEN '${START_DATE}' and '${END_DATE}'
	    order by 1;
      
     .export reset
      
     .os chmod 775 ${TMP_DIR}/${frnchs_get_base_upload_date_range_stg_tbl}_range
      
     .if errorcode = 0 then .goto range_ok;
      
     .os echo "  Error in frnchslib function ${FUNCNAME}"
     .os echo "  failure trying to obtain range";
     .exit 1;
      
   .LABEL range_ok 
    
    .logoff
    .quit 0
!get_date_range
   
}



frnchs_reset_earliest_activity_dte( )
{

  FUNCNAME=frnchs_reset_earliest_activity_dte
  frnchs_reset_earliest_activity_dte_stg_tbl=$1
 
if [[ -z ${frnchs_reset_earliest_activity_dte_stg_tbl} ]] then
  echo ""
  echo "--------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}                                             "
  echo "  Missing history table passed                                                          "
  echo "--------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
bteq<<!resetctl
  .set width 255
	.set format off
	.set titledashes off

   .logon ${HOST}/${USER}, ${PASSWORD}
	DATABASE ${TARGET_DB}; 
	
   update ${TARGET_DB}.DWH_C_FR_CTRL_LOG

      Set Earliest_Activity_Dt = null
      where target_table_name = '${frnchs_reset_earliest_activity_dte_stg_tbl}';

   .if errorcode = 0 then .goto ctl_update_ok;
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .os echo "  failure trying to update ctl table";
   .exit 1;

   .LABEL ctl_update_ok

.logoff;
.quit;
!resetctl
}
 
 
   
   
Tables_Are_Compatible ( )
{
  DATABASE_NAME1=$1
  TABLE_NAME1=$2
  DATABASE_NAME2=$3
  TABLE_NAME2=$4
  SKIPNAMECHECK=$5
   
  if [[ ${SKIPNAMECHECK} = "Y" ]] then 
    cn=
  else
    cn="ColumnName,"
  fi
   
  bteq << !CHECK_TABLE_STRUCTURES
    .logon ${DB_LOGON},${DB_PASSWORD};

     Select ${cn} ColumnFormat, ColumnType, ColumnLength, row_number() over (order by columnid) from dbc.columns
       where Databasename = '${DATABASE_NAME1}'
         and Tablename = '${TABLE_NAME1}'
     minus
     Select ${cn} ColumnFormat, ColumnType, ColumnLength, row_number() over (order by columnid) from dbc.columns
       where Databasename = '${DATABASE_NAME2}'
         and Tablename = '${TABLE_NAME2}';
     .if activitycount > 0 then Exit 1
   
     Select ${cn} ColumnFormat, ColumnType, ColumnLength, row_number() over (order by columnid) from dbc.columns
       where Databasename = '${DATABASE_NAME2}'
         and Tablename = '${TABLE_NAME2}'
     minus
     Select ${cn} ColumnFormat, ColumnType, ColumnLength, row_number() over (order by columnid) from dbc.columns
       where Databasename = '${DATABASE_NAME1}'
         and Tablename = '${TABLE_NAME1}';
     .if activitycount > 0 then Exit 1

    .logoff
    .quit 0
!CHECK_TABLE_STRUCTURES

}   
   
 


 
   

###########################################################################
#                                                                            
#	Function Name:  frnchs_prep_franchise_files
#
#	Description:    Prepares files from franchisee's for unwrap processing
#
# Parameters:     None
#
###########################################################################

frnchs_prep_franchise_files( ) 
{
  FUNCNAME=frnchs_prep_franchise_files
  #frnchs_prep_franchise_files_ftpdir=/data/${ENVR}/exp/frnchs/ftpin
  frnchs_prep_franchise_files_ftpdir=${DATA_DIR}/frnchs/ftpin 
  frnchs_prep_franchise_files_curdir=`echo $PWD`
  cd ${frnchs_prep_franchise_files_ftpdir}
  #find *.txt > /dev/null 2>&1 
  #if [[ $? -eq 0 ]] then
  if [[ $(ls *.txt 2> /dev/null | wc -l) -ne 0 ]] then
    for frnchs_prep_franchise_files_rawfile in `ls -tr *.txt`; do
      cd ${frnchs_prep_franchise_files_ftpdir}
      #
      #  Convert the file from windows crlf to lf
      #
      dos2unix -n ${frnchs_prep_franchise_files_rawfile} lf/${frnchs_prep_franchise_files_rawfile}
      rc=$?
      if [[ ${rc} -gt 0 ]] then
        echo error converting to line feed termination in file ${frnchs_prep_franchise_files_rawfile}
        echo currdir=$PWD
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi

      #
      #  The following sed adds a line feed at the end of the file if it doesn't exist
      #
      sed -i -e '$a\' lf/${frnchs_prep_franchise_files_rawfile}

      #
      #  Find the sequence number
      #
      frnchs_prep_franchise_files_fileseqnbr=`cat ${frnchs_prep_franchise_files_ftpdir}/lf/${frnchs_prep_franchise_files_rawfile} | grep ^1 | grep 1003 | cut -c 5-11|tail -1`
      rc=$?                                                                                                           
      if [[ ${rc} -gt 0 ]] then
        echo error looking for sequence number in file ${frnchs_prep_franchise_files_rawfile}
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi
		
	  #
      #  If frnchs_prep_franchise_files_fileseqnbr empty, raise error
      #
      if [[ -z ${frnchs_prep_franchise_files_fileseqnbr} ]] then
        echo error looking for file sequence number in file name ${frnchs_prep_franchise_files_rawfile}
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi

      #
      #  Find the franchise id
      #
      #frnchs_prep_franchise_files_frnchsid=`echo ${frnchs_prep_franchise_files_rawfile} | grep AXO`
	  #
      #  If variable not empty, this is an AXO file.  Set to AXO's id
      #
	  
	  frnchs_prep_franchise_files_frnchsid=''
	  
      if [[ ${frnchs_prep_franchise_files_rawfile} == *AXO* ]] then
        frnchs_prep_franchise_files_frnchsid=002
      fi

      #
      #  If frnchs_prep_franchise_files_frnchsid still empty, see if it's for FASTCO
      #
      if [[ -z ${frnchs_prep_franchise_files_frnchsid} ]] then
        #frnchs_prep_franchise_files_frnchsid=`echo ${frnchs_prep_franchise_files_rawfile} | grep FASTCO`
        #
        #  If variable not empty, this is an FASTCO file.  Set to FASTCO id
        #
        if [[ ${frnchs_prep_franchise_files_rawfile} == *FASTCO* ]] then
          frnchs_prep_franchise_files_frnchsid=003
        fi
      fi

      #
      #  If frnchs_prep_franchise_files_frnchsid still empty, see if it's for EDCON
      #
      if [[ -z ${frnchs_prep_franchise_files_frnchsid} ]] then
        #frnchs_prep_franchise_files_frnchsid=`echo ${frnchs_prep_franchise_files_rawfile} | grep EDCON`
        #
        #  If variable not empty, this is an EDCON file.  Set to EDCON id
        #
        if [[ ${frnchs_prep_franchise_files_rawfile} == *EDCON* ]] then
          frnchs_prep_franchise_files_frnchsid=005
        fi
      fi

	  #
      #  If frnchs_prep_franchise_files_frnchsid still empty, see if it's for ALSHAYA
      #
      if [[ -z ${frnchs_prep_franchise_files_frnchsid} ]] then
		##        frnchs_prep_franchise_files_frnchsid=`echo ${frnchs_prep_franchise_files_rawfile} | grep ALSHAYA`
        #
        #  If variable not empty, this is an ALSHAYA file.  Set to ALSHAYA id
        #
		##        if [[ -n ${frnchs_prep_franchise_files_frnchsid} ]] then
			if [[ ${frnchs_prep_franchise_files_rawfile} == *ALSHAYA* ]] then
				frnchs_prep_franchise_files_frnchsid=001
			fi
		fi

	  
      #
      #  If frnchs_prep_franchise_files_frnchsid still empty, raise error
      #
      if [[ -z ${frnchs_prep_franchise_files_frnchsid} ]] then
        echo ""
        echo "------------------------------------------------------------------------------------"
        echo "  Error in frnchslib function ${FUNCNAME}                                           "
        echo "  Cannot find franchise string in filename ${frnchs_prep_franchise_files_rawfile}   "
        echo "------------------------------------------------------------------------------------"
        echo ""
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi

      #
      #  Find the file type
      #
      frnchs_prep_franchise_files_filetype=`cat ${frnchs_prep_franchise_files_ftpdir}/lf/${frnchs_prep_franchise_files_rawfile} | grep ^1 | grep 1001 | cut -c 5-15|tail -1`
      rc=$?
      if [[ ${rc} -gt 0 ]] then
        echo ""
        echo "------------------------------------------------------------------------------------"
        echo "  Error in frnchslib function ${FUNCNAME}                                           "
        echo "  looking for file type in filename ${frnchs_prep_franchise_files_rawfile}          "
        echo "------------------------------------------------------------------------------------"
        echo ""
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi
	  
      #
      #  If frnchs_prep_franchise_files_filetype still empty, raise error
      #
      if [[ -z ${frnchs_prep_franchise_files_filetype} ]] then
        echo ""
        echo "------------------------------------------------------------------------------------"
        echo "  Error in frnchslib function ${FUNCNAME}                                           "
        echo "  filetype empty in filename ${frnchs_prep_franchise_files_rawfile}                 "
        echo "------------------------------------------------------------------------------------"
        echo ""
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi

      if [[ ${frnchs_prep_franchise_files_filetype} == INTITEMFR ]] then
        frnchs_prep_franchise_files_inttypecde=ITM 
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTINVFR ]] then
        frnchs_prep_franchise_files_inttypecde=INV
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTTRFCFR ]] then
        frnchs_prep_franchise_files_inttypecde=TRF
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTSTOREFR ]] then
        frnchs_prep_franchise_files_inttypecde=STO
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTPRODTRFR ]] then
        frnchs_prep_franchise_files_inttypecde=PRD
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTTLOGFR ]] then
        frnchs_prep_franchise_files_inttypecde=SLS
      elif [[ ${frnchs_prep_franchise_files_filetype} == INTOPPLNFR ]] then
        frnchs_prep_franchise_files_inttypecde=PLN
      else
        echo ""
        echo "------------------------------------------------------------------------------------"
        echo "  Error in frnchslib function ${FUNCNAME}                                           "
        echo "  unknown filetype in filename ${frnchs_prep_franchise_files_rawfile}               "
        echo "------------------------------------------------------------------------------------"
        echo ""
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      fi
       
      frnchs_prep_franchise_files_interface=FR${frnchs_prep_franchise_files_frnchsid}${frnchs_prep_franchise_files_inttypecde}
      
      #                                  
      #  Format the Interface number and rename the file
      #
      frnchs_prep_franchise_files_filenm=I${frnchs_prep_franchise_files_fileseqnbr}.${frnchs_prep_franchise_files_interface}.${frnchs_prep_franchise_files_rawfile}
      
      #
      #  Copy the file to the prepped folder, with the Sequence number and Interface ID as the first two nodes of the file name
      #
      
	  cat ${frnchs_prep_franchise_files_ftpdir}/lf/${frnchs_prep_franchise_files_rawfile} | sed "s/001${frnchs_prep_franchise_files_filetype}/001${frnchs_prep_franchise_files_interface}/gi" > ${frnchs_prep_franchise_files_ftpdir}/prepped/${frnchs_prep_franchise_files_filenm}  
      if [[ ${rc} -gt 0 ]] then                                                                                                                               
        echo error moving file ${frnchs_prep_franchise_files_rawfile} to prepped folder
        cd ${frnchs_prep_franchise_files_curdir}
        exit 1
      else
        #
        #  Remove the file from the LF folder once it has been successfully moved
        #
        rm -f ${frnchs_prep_franchise_files_ftpdir}/lf/${frnchs_prep_franchise_files_rawfile}
        if [[ ${rc} -gt 0 ]] then                                                                                                                               
          echo error deleting file ${frnchs_prep_franchise_files_rawfile} from lf folder
          cd ${frnchs_prep_franchise_files_curdir}
          exit 1
        fi 
      fi

      #
      #  Move the processed file to the archive folder
      #
	 
      mv ${frnchs_prep_franchise_files_ftpdir}/${frnchs_prep_franchise_files_rawfile} ${frnchs_prep_franchise_files_ftpdir}/archive/${frnchs_prep_franchise_files_rawfile}

    done;
  fi
  cd ${frnchs_prep_franchise_files_curdir}
}
 
 
 
 
 
 
 
 
 
 
 
 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_unwrap_franchise_file
#
#	Description:    Unwraps franchisee's files
#
# Parameters:     None
#
###########################################################################






frnchs_unwrap_franchise_file( ) 
{
  FUNCNAME=frnchs_unwrap_franchise_file
  frnchs_unwrap_franchise_file_intfolder=$1
  frnchs_unwrap_franchise_file_filename=$2
   
  frnchs_unwrap_franchise_file_unwrapname=${DATA_DIR}/frnchs/${frnchs_unwrap_franchise_file_intfolder}/`echo ${frnchs_unwrap_franchise_file_filename}|cut -c1-8`
   echo ${frnchs_unwrap_franchise_file_unwrapname}
  #
  #  Copy file to the interface unwrap folder
  #
  
  cp ${DATA_DIR}/frnchs/ftpin/prepped/${frnchs_unwrap_franchise_file_filename} ${frnchs_unwrap_franchise_file_unwrapname} 
  rc=$?                                                                     
  if [[ ${rc} -gt 0 ]] then
    echo error converting to line feed termination in file ${frnchs_unwrap_franchise_file_rawfile}
    exit 1
  fi
     
  #
  #  Run Unwrap
  #
  . ${ETLHOME}/bin/ir_int_receive.ksh ${frnchs_unwrap_franchise_file_intfolder}
  rc=$?
  echo $rc
  if [[ ${rc} -gt 0 ]] then
    echo error unwrapping file ${frnchs_unwrap_franchise_file_filename}
    exit 1
  else
    echo successfully unwrapped file ${frnchs_unwrap_franchise_file_filename} 
  fi
   
}















###########################################################################
#                                                                            
#	Function Name:  frnchs_set_period_date
#
#	Description:    Calculates and sets the period end date based on the transaction date
#                                                                  
# Parameters:     table name, transaction date, period date column, period type
#
# Example:        frnchs_check_frnchs_shops Name_of_table_to_check 
#
###########################################################################

frnchs_set_period_date( ) 
{
  FUNCNAME=frnchs_set_period_date
  frnchs_set_period_date_tbl=$1
  frnchs_set_period_date_trxn_dt_col=$2
  frnchs_set_period_date_set_col=$3
   
if [[ -z ${frnchs_set_period_date_tbl} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing table passed"
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ -z ${frnchs_set_period_date_trxn_dt_col} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing trxn date column passed"
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ -z ${frnchs_set_period_date_set_col} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing set column passed" 
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
  bteq<<!set_period_date
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
   
   select * 
     from ${SRC_DB}.${frnchs_set_period_date_tbl} tbl
     where tbl.${frnchs_set_period_date_trxn_dt_col} not in
       (select day_key from ${VIEW_DB}.${VIEW_PREFIX}DWH_D_TIM_DAY_LU)
       and tbl.val_dte is not null;
         
   .if activitycount = 0 then .goto date_on_calendar_table 
   .os echo 
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .os echo "       Unable to find date on table ${frnchs_set_period_date_tbl}, column ${frnchs_set_period_date_trxn_dt_col}";
   .os echo "       in calendar table";
   .exit 1
   .Label date_on_calendar_table

   Update tbl
    
     from ${SRC_DB}.${frnchs_set_period_date_tbl} tbl
         ,${VIEW_DB}.${VIEW_PREFIX}DWH_D_TIM_DAY_LU cal
          
     Set ${frnchs_set_period_date_set_col} = 
	  (
            CASE
                WHEN
                    ( '${frnchs_set_period_date_set_col}' = 'wk_end_dte')
                THEN
                    cal.wk_end_dt
                ELSE
                    cal.mth_end_dt
            END
        )      
     where tbl.val_dte is not null
       and tbl.${frnchs_set_period_date_trxn_dt_col} = cal.day_key;
      
   .if errorcode = 0 then .goto fr_period_dateset_ok;
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .os echo "  failure trying to set period end date";
   .exit 1
   
   .LABEL fr_period_dateset_ok; 
    
.logoff;
.quit 0 ;       
 
!set_period_date
 
}

###########################################################################
#                                                                            
#	Function Name:  frnchs_check_date_type
#
#	Description:    Makes sure a date is what it's expected to be
#                                                       
# Parameters:     table name, date_field, date_type
#                 Date type must be one of the following
#                 W  - Week end date
#                 MF - Fiscal Month End 
#
# Example:        frnchs_check_date_type tbl, dt, W 
#
###########################################################################

frnchs_check_date_type( ) 
{
  FUNCNAME=frnchs_check_date_type
  frnchs_check_date_type_tbl=$1
  frnchs_check_date_type_dt_col=$2
  frnchs_check_date_type_dt_type=$3
   
if [[ -z ${frnchs_check_date_type_tbl} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing table passed"
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ -z ${frnchs_check_date_type_dt_col} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing date column passed"
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ -z ${frnchs_check_date_type_dt_type} ]] then
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Missing date type passed" 
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
   
typeset -u frnchs_check_date_type_dt_type
 
if [[ ${frnchs_check_date_type_dt_type} == W || ${frnchs_check_date_type_dt_type} == MF ]] then
  continue
else
  echo ""
  echo "------------------------------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Invalid date type passed" 
  echo "------------------------------------------------------------------------------------"
  echo ""
  exit 1
fi
 
  bteq<<!check_date_type
  .set width 255

  .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
   
   select distinct ${frnchs_check_date_type_dt_col} 
     from  ${frnchs_check_date_type_tbl} 
   minus
   select distinct tbl.${frnchs_check_date_type_dt_col} 
     from  ${frnchs_check_date_type_tbl} tbl
          ,${VIEW_DB}.${VIEW_PREFIX}DWH_D_TIM_DAY_LU          cal
     where cal.day_key = tbl.${frnchs_check_date_type_dt_col}
       and tbl.val_dte is not null
       and case
             when '${frnchs_check_date_type_dt_type}' = 'W' then
               cal.wk_end_dt
             else
               cal.mth_end_dt
             end = cal.day_key;
         
   .if activitycount = 0 then .goto date_ok 
   .os echo 
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .os echo "    Dates do not match date type";
   .exit 1
   .Label date_ok
    
    
.logoff;
.quit;
 
!check_date_type
 
}
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_refresh_all_stats
#
#	Description:    Refresh statistics on teradata
#                                              
# Parameters:     None
#
###########################################################################

frnchs_refresh_all_stats( ) 
{
  FUNCNAME=frnchs_refresh_all_stats
   
  frnchs_refresh_tmp_stats
  rc=$?
  if [ ${rc} -gt 0 ]
  then
     exit 1
  fi
 
  frnchs_refresh_data_stats
  rc=$?
  if [ ${rc} -gt 0 ]
  then
     exit 1
  fi
 
# frnchs_refresh_arc_stats
# rc=$?
# if [ ${rc} -gt 0 ]
# then
#    exit 1
# fi
   
}
 
 
 
 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_refresh_tmp_stats
#
#	Description:    Refresh teradata statistics on tmp database
#
# Parameters:     None
#
###########################################################################

frnchs_refresh_tmp_stats( ) 
{
  FUNCNAME="frnchs_refresh_tmp_stats"
  bteq<<!refresh_tmp_stats
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${SRC_DB};

  .set format off
  .set titledashes off

   Collect Statistics ${SRC_DB}.src_fr_shop_xref           Column (clndr_dte);
   Collect Statistics ${SRC_DB}.src_fr_shop_xref           Column (clndr_dte,frnchs_id,frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_shop_xref           Column (shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_shop_xref           Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_shop_xref           Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_item                Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_item                Column (wk_end_dte,frnchs_id,cntry_cde,short_sku);
   Collect Statistics ${SRC_DB}.src_fr_item                Column (short_sku);
   Collect Statistics ${SRC_DB}.src_fr_item                Column (frnchs_id,short_sku);
   Collect Statistics ${SRC_DB}.src_fr_invonhnd            Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_invonhnd            Column (wk_end_dte,frnchs_id,frnchs_shop_nbr,short_sku);
   Collect Statistics ${SRC_DB}.src_fr_invonhnd            Column (short_sku);
   Collect Statistics ${SRC_DB}.src_fr_invonhnd            Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_invonhnd            Column (shop_nbr);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (activity_dte);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (activity_dte,shop_nbr,bol_invoice_dte,po_nbr,short_sku);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (shop_nbr);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (po_nbr);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (short_sku);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (bol_invoice_dte);
   Collect Statistics ${SRC_DB}.src_ip_intran              Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_trfc                Column (clndr_dte);
   Collect Statistics ${SRC_DB}.src_fr_trfc                Column (clndr_dte,frnchs_id,frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_trfc                Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_trfc                Column (shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_trfc                Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_sls                 Column (trxn_dte);
   Collect Statistics ${SRC_DB}.src_fr_sls                 Column (trxn_dte,frnchs_id,frnchs_shop_nbr,short_sku);
   Collect Statistics ${SRC_DB}.src_fr_sls                 Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_sls                 Column (short_sku);
   Collect Statistics ${SRC_DB}.src_fr_sls                 Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_prdctv_trxn         Column (trxn_dte);
   Collect Statistics ${SRC_DB}.src_fr_prdctv_trxn         Column (trxn_dte,frnchs_id,frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_prdctv_trxn         Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_prdctv_trxn         Column (wk_end_dte);
   Collect Statistics ${SRC_DB}.src_fr_plng                Column (clndr_dte);
   Collect Statistics ${SRC_DB}.src_fr_plng                Column (clndr_dte,frnchs_id,frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_plng                Column (frnchs_shop_nbr);
   Collect Statistics ${SRC_DB}.src_fr_plng                Column (shop_nbr);
   Collect Statistics on ${SRC_DB}.src_fr_shop_xref;
   Collect Statistics on ${SRC_DB}.src_fr_item;
   Collect Statistics on ${SRC_DB}.src_fr_invonhnd;
   Collect Statistics on ${SRC_DB}.src_ip_intran;
   Collect Statistics on ${SRC_DB}.src_fr_trfc;
   Collect Statistics on ${SRC_DB}.src_fr_sls;
   Collect Statistics on ${SRC_DB}.src_fr_prdctv_trxn;
   Collect Statistics on ${SRC_DB}.src_fr_plng;

   
     .if errorcode = 0 then .goto tmp_stats_ok; 
     .os echo " Error in frnchslib function ${FUNCNAME}"
     .exit 1;
      
   .LABEL tmp_stats_ok 
      
    .logoff
    .quit 0

!refresh_tmp_stats

}
 

 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_refresh_data_stats
#
#	Description:    Refresh teradata statistics on data database
#
# Parameters:     None
#
###########################################################################

frnchs_refresh_data_stats( ) 
{
  FUNCNAME="frnchs_refresh_data_stats"
  bteq<<!refresh_data_stats
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off

   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_B        Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_B        Column (wk_key,wf_cus_grp,LOC_COUNTRY_CDE,itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_B        Column (itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_B                Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_B                Column (wk_key,day_key,loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_B                Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_B                Column (loc_key);
   Collect Statistics ${SRC_DB}.STG_D_FR_LOC_DAY_MTX            Column (day_id);
   Collect Statistics ${SRC_DB}.STG_D_FR_LOC_DAY_MTX            Column (day_id,wf_cus_grp,fr_loc_id);
   Collect Statistics ${SRC_DB}.STG_D_FR_LOC_DAY_MTX            Column (loc_id);
   Collect Statistics ${SRC_DB}.STG_D_FR_LOC_DAY_MTX            Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_HST_B                 Column (wk_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_HST_B                 Column (wk_end_dt,wf_cus_grp,LOC_COUNTRY_CDE,itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_HST_B                 Column (itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_ITM_WK_HST_B                 Column (wf_cus_grp,itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B             Column (wk_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B             Column (wk_end_dt,wf_cus_grp,fr_loc_id,itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B             Column (itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B             Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B             Column (loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (day_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (day_dt,loc_id,BOL_INVC_DT,po_num,itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (po_num);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (itm_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (BOL_INVC_NUM);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B               Column (wk_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B                 Column (day_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B                 Column (day_dt,wf_cus_grp,fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B                 Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B                 Column (loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B                 Column (wk_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_HST_B          Column (day_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_HST_B          Column (day_dt,wf_cus_grp,loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_HST_B          Column (loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_HST_B          Column (wk_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MLD_HST_B                Column (day_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MLD_HST_B                Column (day_id,wf_cus_grp,fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MLD_HST_B                Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MLD_HST_B                 Column (loc_id);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_SLS_ILD_B              Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_SLS_ILD_B              Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_SLS_ILD_B              Column (day_key,loc_key,itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_SLS_ILD_B              Column (itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_SLS_ILD_B              Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_WK_MTX                    Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_WK_MTX                    Column (wk_key,loc_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_WK_MTX                    Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_WK_MTX                    Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_DAY_MTX                    Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_DAY_MTX                    Column (day_key,loc_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_DAY_MTX                    Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_D_FR_LOC_DAY_MTX                    Column (fr_loc_id);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG            Column (RUN_CYC_START_TS);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG            Column (msg_ts);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_B         Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_B         Column (wk_key,day_key,loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_B         Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PROSLS_LD_B         Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MER_LD_B                Column (mth_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MER_LD_B                Column (mth_key,day_key,loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MER_LD_B                Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_PLN_MER_LD_B                Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_B              Column (wk_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_B              Column (wk_key,loc_key,itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_B              Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_INV_ILW_B              Column (itm_key);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_POST_LOAD_RPT        Column (job_name);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_POST_LOAD_RPT        Column (job_name,run_dt);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_EXCH_RATE_LU            Column (FROM_CNCY_CDE);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_EXCH_RATE_LU            Column (FROM_CNCY_CDE,eff_dt,TO_CNCY_CDE);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_EXCH_RATE_LU            Column (EFF_DT);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_EXCH_RATE_LU            Column (TO_CNCY_CDE);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILW_A        Column (WK_KEY);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILW_A        Column (WK_KEY,wf_cus_grp,loc_key,itm_key,po_num);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILW_A        Column (itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILW_A        Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILW_A        Column (po_num);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_B        Column (day_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_B        Column (wf_cus_grp);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_B        Column (loc_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_B        Column (itm_key);
   Collect Statistics ${TARGET_DB}.DWH_F_FR_IT_ILD_B        Column (day_key,wf_cus_grp,loc_key,itm_key,po_num);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_DMNRNG_VALUE_LU        Column (dmn_name,seq_num);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_DMNDEF_LU          Column (dmn_name,dmn_scope);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CURR_TIM_LU      Column (CYC_FREQ, eff_end_dt);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CTRL_LOG                        Column (wf_cus_grp);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CTRL_LOG                        Column (wf_cus_grp, target_table_name);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CTRL_LOG                        Column (stts_cde);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CTRL_LOG                        Column (run_freq_cde);
   Collect Statistics ${TARGET_DB}.DWH_C_FR_CTRL_LOG                        Column (earliest_activity_dt);
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_ITM_WK_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_TRFC_LD_B;
   Collect Statistics on ${SRC_DB}.STG_D_FR_LOC_DAY_MTX;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_ITM_WK_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_INV_ILW_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_IT_ILD_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_TRFC_LD_HST_B   ;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_SLS_ILD_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_PROSLS_LD_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_PLN_MLD_HST_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_SLS_ILD_B;
   Collect Statistics on ${TARGET_DB}.DWH_D_FR_LOC_WK_MTX;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_PROSLS_LD_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_PLN_MER_LD_B;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_INV_ILW_B;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_POST_LOAD_RPT;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_EXCH_RATE_LU;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_IT_ILW_A;
   Collect Statistics on ${TARGET_DB}.DWH_F_FR_IT_ILD_B;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_DMNRNG_VALUE_LU;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_DMNLST_VALUE_LU;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_DMNDEF_LU;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_CURR_TIM_LU;
   Collect Statistics on ${TARGET_DB}.DWH_C_FR_CTRL_LOG;
   
   .if errorcode = 0 then .goto data_stats_ok 
   .os echo 
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .exit 1
   .Label data_stats_ok
    
    
.logoff;
.quit 0;
 
!refresh_data_stats

}
 
 
 ###########################################################################
#                                                                            
#	Function Name:  frnchs_check_skus
#
#	Description:    Validates the skus from a source table and logs errors to the error log
#
# Parameters:     table name
#
# Example:        frnchs_check_skus Name_of_table_to_check 
#
###########################################################################

frnchs_check_skus( ) 
{
  FUNCNAME=frnchs_check_skus
  frnchs_check_skus_tbl=$1
  frnchs_check_skus_dtfield=$2
  frnchs_check_skus_dtgrain=$3
   
  if [[ -z ${frnchs_check_skus_tbl} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing table name passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  if [[ -z ${frnchs_check_skus_dtfield} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing date field passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  if [[ -z ${frnchs_check_skus_dtgrain} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing date grain field passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  typeset -u frnchs_check_skus_dtgrain
   
  if [[ ${frnchs_check_skus_dtgrain} != [DW] ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Invalid date grain field passed"
    echo "  date grain must be D (daily) or W (weekly)"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
  
if [[ ${frnchs_check_skus_tbl} = "src_fr_sls" || ${frnchs_check_skus_tbl} = "src_fr_invonhnd" || ${frnchs_check_skus_tbl} = "src_fr_item" ]] then
bteq << !ck_inv_sku
    .set width 255
    
    .logon ${HOST}/${USER}, ${PASSWORD}
		DATABASE ${TARGET_DB};
    
    .format off

     /* 
        Check if there are invalid skus in the item, inv or sales file sent from a franchisee 
        and if so move them to an 'invalid skus' table
     */   
     INSERT INTO ${SRC_DB}.invalid_sku_${frnchs_check_skus_tbl}
     SELECT TBL.*
          , NULL
     FROM ${SRC_DB}.${frnchs_check_skus_tbl} TBL
     WHERE short_sku IN (
       SELECT short_sku
       FROM (Select src.short_sku as short_sku
                ,Sum(case
                   when x.itm_key is null then 1
                   else 0
                 end)          as missing_item
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_skus_tbl}         src  
                      inner join ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_CTRL_LOG ctl
                            on ctl.source_table_name  = '${frnchs_check_skus_tbl}'
                            and ctl.wf_cus_grp        = src.frnchs_id
                      left outer join ${VIEW_DB}.${VIEW_PREFIX}DWH_D_PRD_ITM_LU  x
                              on trim(src.short_sku) = trim(leading '0' from x.itm_id)
                      where (x.itm_id is null )
                   and ctl.stts_cde in ('A', 'R')
                   and src.val_dte is not null
                 group by 1) src
       WHERE src.missing_item <> 0
	   );

    .if errorcode = 0 then .goto no_invalid_sku;
    .os echo "  Error in frnchslib function ${FUNCNAME}"
    .os echo "  Failure moving invalid skus from ${frnchs_check_skus_tbl}";
    .exit 1;
       
    .LABEL no_invalid_sku
     
    .logoff
    .quit 0
 
!ck_inv_sku
fi

if [[ ${frnchs_check_skus_tbl} = "src_fr_sls" || ${frnchs_check_skus_tbl} = "src_fr_invonhnd" || ${frnchs_check_skus_tbl} = "src_fr_item" ]] then
bteq << !rmv_invalid_skus
    .set width 255
    
    .logon ${HOST}/${USER}, ${PASSWORD}
		DATABASE ${TARGET_DB};
    
    .format off

     /* 
        Remove the invalid skus from the base SRC_xxx table
     */  
     DELETE FROM ${SRC_DB}.${frnchs_check_skus_tbl}
     WHERE short_sku IN (
       SELECT short_sku
       FROM (Select src.short_sku as short_sku
                ,Sum(case
                   when x.itm_key is null then 1
                   else 0
                 end)          as missing_item
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_skus_tbl}         src  
                      inner join ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_CTRL_LOG ctl
                            on ctl.source_table_name  = '${frnchs_check_skus_tbl}'
                            and ctl.wf_cus_grp        = src.frnchs_id
                      left outer join ${VIEW_DB}.${VIEW_PREFIX}DWH_D_PRD_ITM_LU  x
                              on trim(src.short_sku) = trim(leading '0' from x.itm_id)
                      where (x.itm_id is null )
                   and ctl.stts_cde in ('A', 'R')
                   and src.val_dte is not null
                 group by 1) src
       WHERE src.missing_item <> 0
	);

    .if errorcode = 0 then .goto no_invalid_sls_sku;
    .os echo "  Error in frnchslib function ${FUNCNAME}"
    .os echo "  Failure moving invalid skus from ${frnchs_check_skus_tbl}";
    .exit 1;
       
    .LABEL no_invalid_sls_sku
     
    .logoff
    .quit 0
!rmv_invalid_skus
fi


  bteq<<!frnchs_check_skus
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
       
   /*
      Now check skus on fr_item against IP item and xref 
   */
   Insert into ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
     (
       run_cyc_start_ts
      ,msg_ts
      ,script_name
      ,"table_name"
      ,msg
      ,process_interrupt_ind
     )
   Select (Select max(RUN_CYC_START_TS)
             from ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_PRE_STG_ERR_LOG
             where script_name = '${SCRIPT_NAME}')
         ,current_timestamp
         ,'${SCRIPT_NAME}'
         ,'${frnchs_check_skus_tbl}'          
         ,'Short sku '||src.short_sku||' is not found on Product Item; Quantity='|| missing_item
         ,'Y'
       from
         (Select src.short_sku as short_sku
                ,Sum(case
                   when x.itm_key is null then 1
                   else 0
                 end)          as missing_item
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_skus_tbl}         src  
                      inner join ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_CTRL_LOG ctl
                            on ctl.source_table_name  = '${frnchs_check_skus_tbl}'
                            and ctl.wf_cus_grp        = src.frnchs_id
                      left outer join ${VIEW_DB}.${VIEW_PREFIX}DWH_D_PRD_ITM_LU  x
                              on trim(src.short_sku) = trim(leading '0' from x.itm_id)
                      where (x.itm_id is null )
                   and ctl.stts_cde in ('A', 'R')
                   and src.val_dte is not null
                 group by 1) src;
    
   .if errorcode = 0 then .goto sku_checking_ok;
   .os echo "  Error in frnchslib function ${FUNCNAME}"
   .os echo "  failure trying to check skus for table ${frnchs_check_skus_tbl}";
   .exit 1
      
   .LABEL sku_checking_ok; 
    
.logoff; 
.quit 0;
 
!frnchs_check_skus

# /* fastco table does not exist in our datamodel so this step has been skipped */
#   /*
#      First remove erroneous test skus from fastco.
#   */
#   Delete tbl
#    
#     from  ${FR_TMP_DATABASE}.${frnchs_check_skus_tbl} tbl
#    
#     where frnchs_id = 3
#       and short_sku in (Select short_sku from ${FR_TMP_DATABASE}.fastco_missing_short_skus  
#                                where reinstate_dte is null or 
#                                      reinstate_dte > tbl.wk_end_dte)
#       and val_dte is not null;
#          
#   .if errorcode = 0 then .goto fastco_fr_sku_removal_ok;
#   .os echo "  Error in frnchslib function ${FUNCNAME}"
#   .os echo "  failure removing fastco missing skus on ${frnchs_check_skus_tbl}";
#   .exit 1
#      
#   .LABEL fastco_fr_sku_removal_ok;  
 
}
 
 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_check_id
#
#	Description:    Validates the franchise id from a source table and logs errors to the error log 
#
# Parameters:     table name
#
# Example:        frnchs_check_id Name_of_table_to_check 
#
###########################################################################

frnchs_check_id( )
{
  FUNCNAME=frnchs_check_id
  frnchs_check_id_tbl=$1
   
  if [[ -z ${frnchs_check_id_tbl} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing table name passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  bteq<<!checkfrid
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
   
   /*
     Make sure franchs_id is legit
   */
   Insert into ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
     (
       RUN_CYC_START_TS
      ,msg_ts
      ,script_name
      ,table_name
      ,msg
      ,process_interrupt_ind  
     ) 
   Select (Select max(run_cyc_start_ts)
             from ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_PRE_STG_ERR_LOG
             where script_name = '${SCRIPT_NAME}')
         ,current_timestamp
         ,'${SCRIPT_NAME}'
         ,'${frnchs_check_id_tbl}'
         ,'Franchise id '||src.FRNCHS_ID||' is not found on DWH_D_ORG_LOC_ATTR_LU; Qty='||trim(cast(src.qty as varchar(20)))
         ,'Y'
       from
         (Select src.FRNCHS_ID as FRNCHS_ID
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_id_tbl} src  
                      left outer join (Select distinct WF_CUS_GRP from ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_ATTR_LU) loc 
                        on src.FRNCHS_ID  = loc.WF_CUS_GRP
                 where loc.WF_CUS_GRP is null
                   and src.val_dte is not null
                 group by 1) src;
    
   .if errorcode = 0 then .goto frnchs_id_check_ok;
   .os echo " Error in frnchs lib function ${FUNCNAME}";
   .os echo " failure trying to check franchise id for table ${frnchs_check_id_tbl}";
   .exit 1
      
   .LABEL frnchs_id_check_ok; 
    
.logoff;
.quit 0;
 
!checkfrid
 
}
 
 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_check_express_shops
#
#	Description:    Validates the express shop from a source table and logs errors to the error log
#                                                                             
# Parameters:     table name
#
# Example:        frnchs_check_express_shops Name_of_table_to_check 
#
###########################################################################

frnchs_check_express_shops( ) 
{
  FUNCNAME=frnchs_check_express_shops
  frnchs_check_express_shops_tbl=$1
   
  if [[ -z ${frnchs_check_express_shops_tbl} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing table name passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  bteq<<!frnchs_check_express_shops
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
   
   /*
     Check Shop on shopinfo
   */
   Insert into ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
     (
       RUN_CYC_START_TS
      ,msg_ts
      ,script_name
      ,table_name
      ,msg
      ,process_interrupt_ind
     )
   Select (Select max(RUN_CYC_START_TS)
             from ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_PRE_STG_ERR_LOG
             where script_name = '${SCRIPT_NAME}')
         ,current_timestamp
         ,'${SCRIPT_NAME}'
         ,'${frnchs_check_express_shops_tbl}'
         ,'Franchise '||src.frnchs_id||'; Shop '||src.shop||' (column shop_nbr) is not found on DWH_D_ORG_LOC_ATTR_LU; Qty='||trim(cast(src.qty as varchar(20)))
         ,'Y'
       from
         (Select src.shop_nbr    as shop
                ,src.frnchs_id   as frnchs_id
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_express_shops_tbl} src  
						LEFT OUTER JOIN  ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_ATTR_LU locattr
						  on src.frnchs_id  = locattr.wf_cus_grp
						  
						LEFT OUTER JOIN ${VIEW_DB}.${VIEW_PREFIX}DWH_D_ORG_LOC_LU si
					    on src.shop_nbr   = si.LOC_ID
						and si.loc_key =locattr.loc_key					            
                   where si.loc_key is null
                and src.val_dte is not null
                   and locattr.wf_cus_grp is null                 
                 group by 1,2) src;
    
   .if errorcode = 0 then .goto shopinfo_checking_ok;
   .os echo " Error in frnchs lib function ${FUNCNAME}";
   .os echo " failure trying to check shopinfo franchise id for table ${frnchs_check_express_shops_tbl}";
   .exit 1
      
   .LABEL shopinfo_checking_ok; 
    
.logoff;
.quit 0;
 
!frnchs_check_express_shops
 
}
 
 
 
 
###########################################################################
#                                                                            
#	Function Name:  frnchs_check_frnchs_shops
#
#	Description:    Validates the franchise shop from a source table and logs errors to the error log
#
# Parameters:     table name
#
# Example:        frnchs_check_frnchs_shops Name_of_table_to_check 
#
###########################################################################

frnchs_check_frnchs_shops( ) 
{
  FUNCNAME=frnchs_check_frnchs_shops
  frnchs_check_frnchs_shops_tbl=$1
  frnchs_check_frnchs_shops_dt_col=$2
  frnchs_check_frnchs_shops_dt_adjust=$3
   
  if [[ -z ${frnchs_check_frnchs_shops_tbl} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing table name passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
  if [[ -z ${frnchs_check_frnchs_shops_dt_col} ]] then
    echo ""
    echo "--------------------------------------------------------------------------------------"
    echo "  Error in frnchslib function ${FUNCNAME}"
    echo "  Missing date column name passed"
    echo "--------------------------------------------------------------------------------------"
    echo ""
    exit 1
  fi
   
   
  bteq<<!check_frnchs_shops
  .set width 255

   .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

  .set format off
  .set titledashes off
   
   /*
     Check Frnchs Shop on fr_shop_xref
   */
   Insert into ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
     (
       RUN_CYC_START_TS
      ,msg_ts
      ,script_name
      ,table_name
      ,msg
      ,process_interrupt_ind
     )
   Select (Select max(RUN_CYC_START_TS)
             from ${VIEW_DB}.${VIEW_PREFIX}DWH_C_FR_PRE_STG_ERR_LOG
             where script_name = '${SCRIPT_NAME}')
         ,current_timestamp
         ,'${SCRIPT_NAME}'
         ,'${frnchs_check_frnchs_shops_tbl}'
         ,'Franchise '||src.frnchs_id||'; Franchise Shop '||src.shop||' (column frnchs_shop_nbr) is not found on DWH_D_FR_LOC_DAY_MTX or SRC_FR_SHOP_XREF; Qty='||trim(cast(src.qty as varchar(20)))
         ,'Y'
       from
         (Select src.frnchs_shop_nbr as shop
                ,src.frnchs_id   as frnchs_id
                ,count(*)      as qty
                 from ${SRC_DB}.${frnchs_check_frnchs_shops_tbl} src  
                      left outer join 
                        (Select FR_LOC_ID
                               ,WF_CUS_GRP
                               ,DAY_KEY AS DAY_ID
                           from ${TARGET_DB}.DWH_D_FR_LOC_DAY_MTX              
                         union                                     
                         Select cast(frnchs_shop_nbr as varchar(10)) 
                              ,cast (frnchs_id as decimal(10,0))
                               ,clndr_dte AS DAY_ID
                           from ${SRC_DB}.src_fr_shop_xref      
                           where val_dte is not null
                        ) shoplist   
                        on src.frnchs_shop_nbr = shoplist.FR_LOC_ID
                       and src.frnchs_id       = shoplist.WF_CUS_GRP
                       and src.${frnchs_check_frnchs_shops_dt_col} = shoplist.DAY_ID
                 where shoplist.FR_LOC_ID is null
                   and src.val_dte is not null
                   ${frnchs_check_frnchs_shops_dt_adjust}
                 group by 1,2) src;
    
   .if errorcode = 0 then .goto fr_shop_checking_ok;
   .os echo " Error in frnchs lib function ${FUNCNAME}";
   .os echo " failure trying to check franchise shops table ${frnchs_check_frnchs_shops_tbl}";
   .exit 1
      
   .LABEL fr_shop_checking_ok; 
    
.logoff;
.quit 0;
 
!check_frnchs_shops
 
}
 
frnchs_control_initial_log_entry_for_run()
{
  FUNCNAME=frnchs_control_initial_log_entry_for_run
 
frnchs_control_initial_log_entry_for_run_mode=$1
if [[ -z ${frnchs_control_initial_log_entry_for_run_mode} ]] then
  frnchs_control_initial_log_entry_for_run_mode=A
fi
 
typeset -u frnchs_control_initial_log_entry_for_run_mode
if [[ ${frnchs_control_initial_log_entry_for_run_mode} != [AD] ]] then
  echo ""
  echo "--------------------------------------------------------------"
  echo "  Error in frnchslib function ${FUNCNAME}"
  echo "  Invalid Mode passed, must be A (add) or D (delete)"
  echo "--------------------------------------------------------------"
  echo ""
  exit 1
fi
   
if [[ ${frnchs_control_initial_log_entry_for_run_mode} = A ]] then
  bteq << !prime_message_tbl
    .set width 255
     
    .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

    .format off
     
     Insert into ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
      ( RUN_CYC_START_TS
       ,msg_ts
       ,script_name
       ,table_name
       ,process_interrupt_ind
       ,msg)
      values (
        current_timestamp(0)
       ,current_timestamp(0)
       ,'${SCRIPT_NAME}'
       ,''
       ,'N'
	   ,'Starting Run'
      );
        
     .if errorcode = 0 then .goto prime_msg_ok; 
     .os echo " Error in frnchslib function ${FUNCNAME}"
     .os echo " failure trying to write prime message to the database";
     .os echo " Msg=${msg}"
     .exit 1;
      
   .LABEL prime_msg_ok 
      
    .logoff
    .quit 0
!prime_message_tbl
else
  bteq << !remove_prime_message
    .set width 255
     
    .logon ${HOST}/${USER}, ${PASSWORD}
   DATABASE ${TARGET_DB};

    .format off
     
     Delete from ${TARGET_DB}.DWH_C_FR_PRE_STG_ERR_LOG
       where msg        = 'Starting Run'
         and script_name = '${SCRIPT_NAME}';
    
   .if errorcode = 0 then .goto clear_log_row_ok;
   .os echo " Error in frnchslib function ${FUNCNAME}"
   .os echo " failure trying to clear initial log row";
   .exit 1
      
   .LABEL clear_log_row_ok; 
          
      
    .logoff
    .quit 0
!remove_prime_message
fi
 
}
    
   
   
   
   
  
