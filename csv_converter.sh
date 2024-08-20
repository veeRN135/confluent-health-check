
#!/bin/bash

# Set input and output file names
input_file="input.txt"
output_file="output.csv"

# Initialize QMNAME variable
QMNAME=""

# Clear the output file if it exists
> "$output_file"

# Read the input file line by line
while IFS= read -r line; do
  # If the line is empty or consists only of dashes, skip it
  if [[ -z "$line" || "$line" == "----------" || "$line" == "---------------------" ]]; then
    continue
  fi

  # If QMNAME is not set, this line is the QMNAME
  if [[ -z "$QMNAME" ]]; then
    QMNAME="$line"
  else
    # Otherwise, write QMNAME:LINE to the output file
    echo "$QMNAME:$line" >> "$output_file"
  fi
done < "$input_file"

echo "Conversion completed. Output saved to $output_file"