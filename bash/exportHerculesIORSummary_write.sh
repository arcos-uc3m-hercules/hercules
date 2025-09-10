# !/bin/bash

NUMBER_OF_FILES=$1
FILES_LIST=$(ls -tr logs/hercules/* | tail -n${NUMBER_OF_FILES}); 

echo $FILES_LIST

for file in $FILES_LIST; 
do 
	#grep "write\|read" $file | tail -n2;
	out1=$(sed -e 's/ \{1,\}/ /g' $file | grep "\bwrite\b" | tail -n1;)
#	out2=$(sed -e 's/ \{1,\}/ /g' $file | grep "read" | tail -n1;)
	out3=$(sed -e 's/ \{1,\}/ /g' $file | grep "DATA SERVERS" | tail -n1;)
        out4=$(sed -e 's/ \{1,\}/ /g' $file  | grep "BLOCK_SIZE" | awk '{print $7}' | tail -n1;)
	#       echo $out3 $out1
	echo $out4 $out3 $out1
done
