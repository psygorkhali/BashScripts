#!/bin/ksh
#############################################################################
# script      : chk_err.sh
# Description : This library function takes returncode and error message as arguments
#               and ends the programs with the returncode
# Modifications
# 12/27/2012  : Logic  : Initial Script
# 02/24/2017  : Logic  : Added the fucntion for ODS cyclic error. Which will be directed only for the ODS code. 
##############################################################################

function chk_err {

    ret_code=0
    ret_msg="Fatal error"

   while getopts "r:m:" arg
   do
      case $arg in
       r ) ret_code=$OPTARG;;

       m ) ret_msg=$OPTARG;;

       \? ) print 'chk_err usage: -r return_code -m message'
            return 1
      esac
   done

   if [[ $ret_code != 0 ]]
   then
	  print_msg "############################################"
	  print_msg "ERROR found in script ${SCRIPT_NAME}"
      print_msg "ERROR: $ret_msg"
	  print_msg "############################################"
	  
	  print_err "############################################"
	  print_err "ERROR found in script ${SCRIPT_NAME}"
      print_err "ERROR: $ret_msg"
	  print_err "############################################"	  
	  		  
	  NOTIFICATION_TIME=$(date +"%Y-%m-%d %H:%M:%S")
	  export MAIL_SUBJECT="ERROR in script $SCRIPT_NAME.ksh"
	  echo "Error in script $SCRIPT_NAME.ksh executed at $NOTIFICATION_TIME" > $EMAILMESSAGE
	  echo "<HTML><p><font color=red>ERROR: $ret_msg</p></HTML>" >>$EMAILMESSAGE
	  send_mail
      parent_ret_code=$ret_code
      if [[ -n $BATCH_ID && -n $JOB_ID ]]; then
          script_unsuccesful
      fi
	  if [[ -n $CURRENT_LOAD_BATCH_ID && -n $ODS_CYCLIC ]]; then
          ods_cyclic_error
      fi
     exit $parent_ret_code
   fi
}
