#!/bin/bash

# Set input and output file names
input_file="input.txt"
output_file="output.csv"

# Clear the output file if it exists
> "$output_file"

# Initialize QMNAME variable
QMNAME=""
expecting_qmname=false

# Read the input file line by line
while IFS= read -r line; do

  # Skip lines that consist only of underscores
  if [[ "$line" =~ ^_+$ ]]; then
    continue
  fi

  # If the line consists only of dashes, reset QMNAME and indicate that the next line should be the new QMNAME
  if [[ "$line" =~ ^-+$ ]]; then
    expecting_qmname=true
    continue
  fi

  # If we're expecting a QMNAME after dashes, set QMNAME
  if $expecting_qmname; then
    QMNAME="$line"
    expecting_qmname=false
    continue
  fi

  # If QMNAME is set, treat the current line as a data line
  if [[ -n "$QMNAME" ]]; then
    echo "$QMNAME:$line" >> "$output_file"
  fi

done < "$input_file"

echo "Conversion completed. Output saved to $output_file"