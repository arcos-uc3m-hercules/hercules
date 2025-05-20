# !/bin/bash

## Usage:
# ./exportDiskTimes.sh <number_of_files_to_be_read>

NUMBER_OF_FILES=$1
FILES_LIST=$(ls -tr d-server-* | tail -n${NUMBER_OF_FILES}); 

echo $FILES_LIST

for file in $FILES_LIST; 
do 
	#grep "write\|read" $file | tail -n2;
#	out1=$(sed -e 's/ \{1,\}/ /g' $file | sed G | grep "Total writen")
	sed -e 's/ \{1,\}/ /g' $file  | cut -d'>' -f2-
	#| xargs -L1 echo
#	out1=$(sed -e 's/ \{1,\}/ /g' $file | grep "write" | tail -n1;)
#	out2=$(sed -e 's/ \{1,\}/ /g' $file | grep "read" | tail -n1;)
#	out3=$(sed -e 's/ \{1,\}/ /g' $file | grep "DATA SERVERS" | tail -n1;)
#	echo $out3 $out1
#	echo $out3 $out2
#	echo $out1
done
