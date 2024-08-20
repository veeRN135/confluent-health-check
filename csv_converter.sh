#!/bin/bash

# Set input and output file names
input_file="input.txt"
output_file="output.csv"

# Clear the output file if it exists
> "$output_file"

# Initialize QMNAME variable
QMNAME=""

# Read the input file line by line
while IFS= read -r line; do

  # Debugging: print each line being processed
  echo "DEBUG: Processing line: '$line'"

  # Skip lines that consist only of underscores or are empty
  if [[ "$line" =~ ^_+$ || -z "$line" ]]; then
    echo "DEBUG: Skipping underscore or empty line"
    continue
  fi

  # If the line consists only of dashes, prepare for a new QMNAME
  if [[ "$line" =~ ^-+$ ]]; then
    echo "DEBUG: Found dashes, resetting QMNAME"
    QMNAME=""
    continue
  fi

  # If QMNAME is empty, set it with the current line
  if [[ -z "$QMNAME" ]]; then
    QMNAME="$line"
    echo "DEBUG: Setting QMNAME to: '$QMNAME'"
  else
    # Write the data line to the output file with QMNAME prefix
    echo "$QMNAME:$line" >> "$output_file"
    echo "DEBUG: Writing to output: '$QMNAME:$line'"
  fi

done < "$input_file"

echo "Conversion completed. Output saved to $output_file"