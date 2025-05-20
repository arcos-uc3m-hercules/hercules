#!/bin/bash

# Number of files to process
n=4  

# Find the last "n" created files starting with "d-server..."
files=$(ls -t d-server* 2>/dev/null | head -n "$n")
results_file=results.csv

# Check if any files were found
if [[ -z "$files" ]]; then
  echo "No files starting with 'd-server' found."
  exit 1
fi

if [[ ! -f ${results_file} ]]; then
	echo "Number of servers,Server ID,Hostname,Total written(B),Total written(MB),Total written(GB),Time to write(s),Throughput(B/s),Throughput(MB/s),Throughput(GB/s),Blocksize(B),Collecting time(s),Merge time(s),Policy,Results date" > ${results_file}
fi

# Loop through the files
for file in $files; do
  echo "Processing file: $file"

  # Read the file and extract the substring starting with "NumberServers..."
  #substring=$(grep -o 'NumberServers.*' "$file")
  substring=$(cat "$file" | awk -F'>' '{print $2}')

  # Remove leading/trailing spaces if necessary
  substring=$(echo "$substring" | tr -d ' ')

  date=$(cat $file | awk '{print $1}')

  # Check if the substring was found
  if [[ -z "$substring" ]]; then
    echo "No line containing 'NumberServers' found in the file: $file"
  else
    # Print the result
    #echo "Extracted substring from $file:"
    echo "$substring","$date" >> ${results_file}
  fi

  #echo "-------------------------"
done
