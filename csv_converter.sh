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
  # Skip lines consisting only of underscores or dashes
  if [[ "$line" =~ ^_+$ || "$line" =~ ^-+$ ]]; then
    # Reset QMNAME if a separator line (dashes) is encountered
    if [[ "$line" =~ ^-+$ ]]; then
      QMNAME=""
    fi
    continue
  fi

  # If QMNAME is not set, treat the current line as QMNAME
  if [[ -z "$QMNAME" ]]; then
    QMNAME="$line"
  else
    # If QMNAME is set, treat the current line as data line
    echo "$QMNAME:$line" >> "$output_file"
  fi
done < "$input_file"

echo "Conversion completed. Output saved to $output_file"