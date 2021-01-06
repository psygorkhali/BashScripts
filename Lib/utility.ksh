#!/bin/ksh

###############################################################################################
# Script Name: utilty.ksh
#
# Description : This library functions defines functions which helps other functions for
#               certain type of activities like parsing log file.
#
# Modifications
# 12/27/2012  : Logic  : Initial Script
#
#
################################################################################################

####################################################################################################
# Function Name : bteqlog_parse
# Description   : This function parses the result of BTeq query and returns all values in an array
# Returned Value: LAST_ACTIVITY[]
####################################################################################################

function bteqlog_parse {

    formatted_log=$(echo "${_bteqlog}" | awk '
    /^ \*\*\* (\w+) completed\. (\w+) rows?.*/ {
        str=gensub(/^ \*\*\* (\w+) completed\. (\w+) rows?.*/,"\\1 \\2","g",$0);
        sub("One","1",str);
        sub("No","0",str);
        if (getline > 0 && $0 ~ /^ \*\*\* Total elapsed time was (\w+) (\w+).*/) {
            time=gensub(/^ \*\*\* Total elapsed time was (\w+) (\w+).*/," \\1 \\2","g",$0);
        }
        print str""time;
        str=""; time="";
    }
    /^ \*\*\* Failure/ {
        str=gensub(/^ \*\*\* /,"Failure ","1",$0);
        getline;
        while (!($0 ~ /^ *Statement# \w+, Info =\w+/ || $0 ~ /^ \*\*\*/)) {
            errmsg=gensub(/^ *$/,"","g",$0)
            if (errmsg != "")
                str = str""errmsg
            getline;
        }
        print str;
    }
    ')

    unset LAST_ACTIVITY
    formatted_log=`echo "${formatted_log}" | tail -n1`
	
    if [[ ${formatted_log:0:7} == "Failure" ]]
    then
        LAST_ACTIVITY[0]="Error"
        LAST_ACTIVITY[1]="${formatted_log:8}"
    else
        LAST_ACTIVITY=(${formatted_log})
    fi
    
    _bteq_log_parsed=1
}

####################################################################################################
# Function Name : fastloadlog_parse
# Description   : This function parses the result of Fastload query and stores all values in an array
####################################################################################################

function fastloadlog_parse {

    formatted_log=$(
    echo "$fastloadlog" | awk 'BEGIN { command=0; message=0; end_loading=0 }
    /^00[0-9][0-9] /{
        if ( end_loading == 1 )
            exit
			
            { command=1; message=0; }
    }
    /^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*/ {
        if ( command == 1 )
        {
            messagestr=gensub(/^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] (.*)/,"\\1","g",$0);
        
            command=0; message=1;
            if ( messagestr ~ /END LOADING COMPLETE/ )
                { end_loading=1; message=0; }
        }
        else if ( message == 1 )
        {
            tmp=gensub(/^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] (.*)/,"\\1","g",$0);
            if ( tmp ~ /Command not processed/ )
                messagestr=messagestr ". " tmp
            else if ( tmp ~ /Error at record number/ )
                messagestr=messagestr ". " tmp
            else
                messagestr=tmp
        }
    }
    {
        if ( end_loading == 1 ) {
            if ( $0 ~ /Total Records Read *= *[0-9]+/)
                read_count=gensub(/^ *Total Records Read *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /- skipped by RECORD command *= *[0-9]+/)
                skip_count=gensub(/^ *- skipped by RECORD command *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /- sent to the RDBMS *= *[0-9]+/)
                sent_count=gensub(/^ *- sent to the RDBMS *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /Total Error Table 1 *= *[0-9]+/)
                err1_count=gensub(/^ *Total Error Table 1 *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /Total Error Table 2 *= *[0-9]+/)
                err2_count=gensub(/^ *Total Error Table 2 *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /Total Inserts Applied *= *[0-9]+/)
                insert_count=gensub(/^ *Total Inserts Applied *= *([0-9]+).*/,"\\1","g",$0);
            else if ( $0 ~ /Total Duplicate Rows *= *[0-9]+/)
                duplicate_count=gensub(/^ *Total Duplicate Rows *= *([0-9]+).*/,"\\1","g",$0);
        }
        else if ( $0 ~ /^ +=+ *$/ )
            message=0;
        else if ( message == 1 && $0 ~ /^     / && $0 !~ /^[ \t]*$/ )
            messagestr=messagestr " " gensub(/^ +(.*[^ ]) *$/,"\\1","g",$0);
    }
    END {
        print messagestr;
        if ( skip_count == "" && read_count != "" ) { skip_count=0; sent_count=read_count; }
		print read_count,skip_count,sent_count,err1_count,err2_count,insert_count,duplicate_count
    }
    ')
	
	echo FORMATTED_LOG: ${formatted_log}
	echo "OUTPUT FROM fastloadlog_parse"

    unset LAST_ACTIVITY
    LAST_ACTIVITY[0]=$(echo "${formatted_log}" | head -n1)
    LAST_ACTIVITY+=($(echo "${formatted_log}" | tail -n1))
	echo "err1count ="$err1_count
echo "err2count="$err2_count
}

####################################################################################################
# Function Name : mloadlog_parse
# Description   : This function parses the result of Mload query and stores all values in an array
####################################################################################################

function mloadlog_parse {
	
	print_msg "MLOAD PARSE"
	rm ${TMP_DIR}/test_mload
	rm ${TMP_DIR}/test_mload_count.txt
	echo "${mloadlog}" >> ${TMP_DIR}/test_mload
	unset LAST_ACTIVITY
 	
	awk '{print $0}' ${TMP_DIR}/test_mload | grep 'Inserts:' >> ${TMP_DIR}/test_mload_count.txt

	LAST_ACTIVITY[1]=`awk '{print $2}' ${TMP_DIR}/test_mload_count.txt`

	echo "LAST_ACTIVITY[1]=" ${LAST_ACTIVITY[1]}
}



####################################################################################################
# Function Name : mload_log_parse
# Description   : This function parses the result of Mload query and stores all values in an array
####################################################################################################

function mload_log_parse {

print_msg "MLOAD PARSE"
print_msg "MLOAD LOG = ${mloadlog}"
print_msg ""

#   formatted_log=$(
#    echo "$mloadlog" | awk 'BEGIN { command=0; message=0; end_loading=0 }
#    /^00[0-9][0-9] /{
#        if ( end_loading == 1 )
#            exit
#        else
#            { command=1; message=0; }
#    }
#    /^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*/ {
#        if ( command == 1 )
#        {
#            messagestr=gensub(/^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] (.*)/,"\\1","g",$0);
#        
#            command=0; message=1;
#            if ( messagestr ~ /END LOADING COMPLETE/ )
#                { end_loading=1; message=0; }
#        }
#        else if ( message == 1 )
#        {
#            tmp=gensub(/^\*\*\*\* [0-9][0-9]:[0-9][0-9]:[0-9][0-9] (.*)/,"\\1","g",$0);
#            if ( tmp ~ /Command not processed/ )
#                messagestr=messagestr ". " tmp
#            else if ( tmp ~ /Error at record number/ )
#                messagestr=messagestr ". " tmp
#            else
#                messagestr=tmp
#        }
#    }
#    {
#        if ( end_loading == 1 ) {
#            if ( $0 ~ /Total Records Read *= *[0-9]+/)
#                read_count=gensub(/^ *Total Records Read *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /- skipped by RECORD command *= *[0-9]+/)
#                skip_count=gensub(/^ *- skipped by RECORD command *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /- sent to the RDBMS *= *[0-9]+/)
#                sent_count=gensub(/^ *- sent to the RDBMS *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /Total Error Table 1 *= *[0-9]+/)
#                err1_count=gensub(/^ *Total Error Table 1 *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /Total Error Table 2 *= *[0-9]+/)
#                err2_count=gensub(/^ *Total Error Table 2 *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /Total Inserts Applied *= *[0-9]+/)
#                insert_count=gensub(/^ *Total Inserts Applied *= *([0-9]+).*/,"\\1","g",$0);
#            else if ( $0 ~ /Total Duplicate Rows *= *[0-9]+/)
#                duplicate_count=gensub(/^ *Total Duplicate Rows *= *([0-9]+).*/,"\\1","g",$0);
#        }
#        else if ( $0 ~ /^ +=+ *$/ )
#            message=0;
#        else if ( message == 1 && $0 ~ /^     / && $0 !~ /^[ \t]*$/ )
#            messagestr=messagestr " " gensub(/^ +(.*[^ ]) *$/,"\\1","g",$0);
#    }
#    END {
#        print messagestr;
#        if ( skip_count == "" && read_count != "" ) { skip_count=0; sent_count=read_count; }
#		print read_count,skip_count,sent_count,err1_count,err2_count,insert_count,duplicate_count
#    }
#    ')
#	echo "OUTPUT FROM fastloadlog_parse"
#
#    unset LAST_ACTIVITY
#    LAST_ACTIVITY[0]=$(echo "${formatted_log}" | head -n1)
#    LAST_ACTIVITY+=($(echo "${formatted_log}" | tail -n1))
#	echo "err1count ="$err1_count
#	echo "err2count="$err2_count
}



####################################################################################################
# Function Name : set_bteqlog
# Description   : 
####################################################################################################

function set_bteqlog {
    _bteq_log_parsed=0
    _bteqlog=$1
}

####################################################################################################
# Function Name : get_last_error_for_sql
# Description   : 
####################################################################################################

function get_last_error_for_sql {

    bteqlog_parse

    typeset err_str=""
    if [[ ${LAST_ACTIVITY[0]} == "Error" ]]
    then
        err_str=${LAST_ACTIVITY[1]}
    fi

    echo "${err_str//\'/\'\'}"
}

####################################################################################################
# Function Name : get_fastload_error
# Description   : 
# Returned Value: 
####################################################################################################

function get_fastload_error {
    
    echo ${LAST_ACTIVITY[0]//\'/\'\'}
}

####################################################################################################
# Function Name : get_fastload_err1_count
# Description   : Returns number of rejected records in Fastload query
####################################################################################################

function get_fastload_err1_count {
    
    echo ${LAST_ACTIVITY[4]}
}

####################################################################################################
# Function Name : get_fastload_err2_count
# Description   : Returns number of rejected records in Fastload query
####################################################################################################

function get_fastload_err2_count {
    
    echo ${LAST_ACTIVITY[5]}
}

####################################################################################################
# Function Name : get_fastload_insert_count
# Description   : Returns number of inserted records by Fastload query
####################################################################################################

function get_mload_insert_count {
    
    echo ${LAST_ACTIVITY[1]}
}

####################################################################################################
# Function Name : get_fastload_insert_count
# Description   : Returns number of inserted records by Fastload query
####################################################################################################

function get_fastload_insert_count {
    
    echo ${LAST_ACTIVITY[6]}
}

####################################################################################################
# Function Name : get_activity_count
# Description   : 
####################################################################################################

function get_activity_count {
    
    if [[ ${_bteq_log_parsed} -eq 0 ]]; then
        bteqlog_parse
    fi
    
    if [[ ${LAST_ACTIVITY[0]} == "Error" ]]
    then
        echo -1
    else
        echo ${LAST_ACTIVITY[1]}
    fi
}

####################################################################################################
# Function Name : get_activity_type
# Description   : 
####################################################################################################

function get_activity_type {
    
    if [[ ${_bteq_log_parsed} -eq 0 ]]; then
        bteqlog_parse
    fi
    
    echo ${LAST_ACTIVITY[0]}
}
