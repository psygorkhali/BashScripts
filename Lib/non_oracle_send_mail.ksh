#!/bin/ksh

###############################################################################################
# Script Name : send_mail.ksh
# Description : This library functions defines send_mail function which sends the error message
#               and reject message via email to certain group of users.
#               
# Modifications
# 17/11/2014   : Yomari  : Initial Script
################################################################################################

###############Function send_mail is for sending error email######################################
function EmailFile {
		VAR_E_CONSTANT=$1
		VAR_FILE_TO_MAIL=$2
		
		MAIL_SUBJECT=$3
		echo $VAR_FILE_TO_MAIL
	##############Check to see if the mail sending status is on #########################
	
		print_msg "Sending  Mail"
		print_msg "Mail Subject:  "$MAIL_SUBJECT
		print_msg "Mailing To:  "$EMAIL_TO
		#print_msg "Mail message: "$EMAILMESSAGE
		echo "mutt -e set content_type=text/html -s $MAIL_SUBJECT $EMAIL_TO"
		
		#mutt -e "set content_type=text/html" -s "$MAIL_SUBJECT" "$EMAIL_TO" < $EMAILMESSAGE
		echo $EMAILMESSAGE | mutt  -a  "$VAR_FILE_TO_MAIL" -s "$MAIL_SUBJECT" "$EMAIL_TO" 
		print_msg "Mail Sent"
	
}

EmailFile_Multiple_Contacts ( ) 
  {
  
   VAR_E_CONSTANT=$1
   VAR_FILE_TO_MAIL=$2
   VAR_SUBJECT=$3

   VAR_SEND_MAIL_KSH=${TMP_DIR}/ir_send_mail_${VAR_E_CONSTANT}_${PPID}.ksh

##########################################################################
#
# Build a UNIX script that will e-mail appropriate contacts
# based in their contact level.
#
##########################################################################

bteq<<!BUILD_EMAIL_LIST
   
   .logon ${HOST}/${USER}, ${PASSWORD}
	DATABASE ${TARGET_DB};

   .set width 254
   .export file=$VAR_SEND_MAIL_KSH

   SELECT 'cat $VAR_FILE_TO_MAIL | mailx -s "$VAR_SUBJECT" '||'"'||
           TRIM(BOTH FROM c.email_address)||'"' (TITLE '')
     FROM ${TARGET_DB}.DWH_C_EMAIL_CONTACTS c
         ,${TARGET_DB}.DWH_C_EMAIL_CONSTANT ec
    WHERE c.contact_level BETWEEN ec.min_level
                              AND ec.max_level
      AND ec.constant_name = '$VAR_E_CONSTANT'
      AND ec.EFF_DT = (SELECT MAX(EFF_DT)
                                 FROM ${TARGET_DB}.DWH_C_EMAIL_CONSTANT
                                WHERE CONSTANT_NAME = '$VAR_E_CONSTANT'
                                  AND EFF_DT <= '$CURR_DAY');

   .export reset
  
   .IF ERRORLEVEL <> 0 THEN .EXIT 4; 
   .IF ACTIVITYCOUNT <> 0 THEN .GOTO err1_check_ok;
   
   .quit 4;

   .LABEL err1_check_ok;
   .logoff;
   .quit;

!BUILD_EMAIL_LIST

echo $VAR_SEND_MAIL_KSH

if [ $? = 4 ]
then
   echo "Error building e-mail distribution..."
   echo "rm $VAR_SEND_MAIL_KSH"
   echo "rm $VAR_FILE_TO_MAIL"
   
   rm $VAR_SEND_MAIL_KSH
   rm $VAR_FILE_TO_MAIL
   
   exit 1
fi 

   echo "               "
   echo "Sending e-mails"
   chmod a+x $VAR_SEND_MAIL_KSH 
   $VAR_SEND_MAIL_KSH
   
   echo "rm $VAR_SEND_MAIL_KSH"
   echo "rm $VAR_FILE_TO_MAIL"
   
   rm $VAR_SEND_MAIL_KSH
   rm $VAR_FILE_TO_MAIL
   
  }


###############Function reject_mail is for sending warning email######################################
function reject_mail {
	##############Check to see if the mail sending status is on #########################
	if [[ $EMAIL_REJECT -eq 1 ]]
	then
		print_msg "Sending Reject Mail"
		print_msg "Mail Subject:  "$MAIL_SUBJECT
		print_msg "Mailing To:  "$REJECT_TO
		print_msg "Mail message: "$EMAILMESSAGE
		/bin/mail -s "$MAIL_SUBJECT" "$REJECT_TO" < $EMAILMESSAGE
		print_msg "Reject Mail Sent"
	fi
}



#################Function send_script_run_mail is for sending the script run status #################################
function send_script_run_mail {
	NOTIFICATION_TIME=$(date +"%Y-%m-%d %H:%M:%S")

	MAIL_SUBJECT="Script: $SCRIPT_NAME.ksh Run Status"
	##############Check to see if the mail sending status is on #########################
	if [[ $EMAIL_NOTIFICATION_STATUS -eq 1 ]]
	then
		if [[ $1 -eq 1 ]]
		then
			echo "Script: $SCRIPT_NAME.ksh Started at $NOTIFICATION_TIME" > $EMAILMESSAGE
			echo "Sending Script Start run Mail"
		elif [[ $1 -eq 22 ]]
		then
			echo "Script: $SCRIPT_NAME.ksh was tried to re run again at the same day" > $EMAILMESSAGE
			echo "Sending re-run Mail"
		else
			echo "Script: $SCRIPT_NAME.ksh Completed at $NOTIFICATION_TIME" > $EMAILMESSAGE
			echo "Sending Script Complete run Mail"
		fi

		echo "Mail Subject:  "$MAIL_SUBJECT
		echo "Mailing To:  "$EMAIL_NOTIFICATION_LIST
		/bin/mail -s "$MAIL_SUBJECT" "$EMAIL_NOTIFICATION_LIST" < $EMAILMESSAGE
		echo "Script status mail sent"
	fi
}


#################Function send_batch_run_mail is for sending the batch run status #################################
function send_batch_run_mail {
	NOTIFICATION_TIME=$(date +"%Y-%m-%d %H:%M:%S")
	MAIL_BATCH_SUBJECT="Batch with batch id $BATCH_ID Run Status"
	if [[ $1 -eq 11 ]]
	then 
		echo "Batch with batch id $BATCH_ID Started at $NOTIFICATION_TIME" > $EMAILMESSAGE
		echo "Sending Batch start run status mail"
	elif [[ $1 -eq 12 ]]
	then
		echo "Batch with batch id $BATCH_ID Completed at $NOTIFICATION_TIME" > $EMAILMESSAGE
		echo "Sending Batch complete run status mail"
	fi
	
	
	##############Check to see if the mail sending status is on #########################
	if [[ $EMAIL_BATCH_NOTIFICATION_STATUS -eq 1 ]]
	then
		echo "Mail Subject:  "$MAIL_BATCH_SUBJECT
		echo "Mailing To:  "$EMAIL_NOTIFICATION_LIST
		/bin/mail -s "$MAIL_BATCH_SUBJECT" "$EMAIL_NOTIFICATION_LIST" < $EMAILMESSAGE
		echo "Batch status mail sent"
	fi
}