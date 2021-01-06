#!/bin/sh
#############################################################################
# script : print_msg.sh                                                     #
# Description : This library function takes a string as argument and        #
#               prints it in the log file                                   #
# Modifications                                                             #
# 12/27/2012  : Logic  : Initial Script                                    #
#                                                                           #
#############################################################################

function print_msg
{
   echo "$SCRIPT_NAME "`date +"%m/%d/%Y %T"`": $1"  >> $LOG_FILE
}



function print_err
{
	echo "$SCRIPT_NAME "`date +"%m/%d/%Y %T"`": $1"  >> $ERROR_FILE
	
	echo "$SCRIPT_NAME "`date +"%m/%d/%Y %T"`": $1" >&2
}


function print_bookmark
{
	echo ""  >> $LOG_FILE
	echo ""  >> $LOG_FILE
	echo "==============================================================================================================================================">>$LOG_FILE
	echo "====================================================  $1  ================================================">>$LOG_FILE
	echo "==============================================================================================================================================">>$LOG_FILE
	echo ""  >> $LOG_FILE
	echo ""  >> $LOG_FILE
}