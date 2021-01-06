#!/bin/ksh

#################################################################################################
# Script Name : send_mail.ksh                                                                   #
# Description : This library functions defines send_mail function which sends the error message #
#               and reject message via email to certain group of users.                         #
#                                                                                               #
# Modifications                                                                                 #
# 17/11/2014   : Logic  : Initial Script                                                       #
#################################################################################################


############################################################################################################
# Function Name : send_mail
# Description   : This function will send error mail to the specified recipients. 
# 11/19/2014 
############################################################################################################

function send_mail {
	##############Check to see if the mail sending status is on #########################
	if [[ $EMAIL_ERROR -eq 1 ]]
	then
		print_msg "Sending Error Mail"
		print_msg "Mail Subject:  "$MAIL_SUBJECT
		print_msg "Mailing To:  "$EMAIL_TO
		print_msg "Mail message: "$EMAILMESSAGE
		echo "mutt -e set content_type=text/html -s $MAIL_SUBJECT $EMAIL_TO < $EMAILMESSAGE"
		
		mutt -e "set content_type=text/html" -s "$MAIL_SUBJECT" "$EMAIL_TO" < $EMAILMESSAGE
		print_msg "Error Mail Sent"
	fi
}

############################################################################################################
# Function Name : reject_mail
# Description   : This function will send reject mail as warning to the specified recipients. 
# 11/19/2014 
############################################################################################################

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


############################################################################################################
# Function Name : send_script_run_mail
# Description   : This function will send script run status mail as notification to the specified recipients. 
# 11/19/2014 
############################################################################################################

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

############################################################################################################
# Function Name : send_batch_run_mail
# Description   : This function will send batch run status  mail as notification to the specified recipients. 
# 11/19/2014 
############################################################################################################

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