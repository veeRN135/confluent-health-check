# File paths
file1_path = "file1.txt"
file2_path = "file2.txt"

# Function to load file data into a dictionary
def load_offsets(file_path):
    offsets = {}
    with open(file_path, "r") as file:
        for line in file:
            topic, partition, offset = line.strip().split(":")
            offsets[(topic, partition)] = int(offset)
    return offsets

# Load offsets from both files
offsets_file1 = load_offsets(file1_path)
offsets_file2 = load_offsets(file2_path)

# Find mismatched offsets
mismatches = []
for key in offsets_file1:
    if key in offsets_file2:
        if offsets_file1[key] != offsets_file2[key]:
            mismatches.append({
                "topic": key[0],
                "partition": key[1],
                "file1_offset": offsets_file1[key],
                "file2_offset": offsets_file2[key],
                "difference": abs(offsets_file1[key] - offsets_file2[key])
            })
    else:
        mismatches.append({
            "topic": key[0],
            "partition": key[1],
            "file1_offset": offsets_file1[key],
            "file2_offset": "Missing",
            "difference": "N/A"
        })

# Include entries missing in file1 but present in file2
for key in offsets_file2:
    if key not in offsets_file1:
        mismatches.append({
            "topic": key[0],
            "partition": key[1],
            "file1_offset": "Missing",
            "file2_offset": offsets_file2[key],
            "difference": "N/A"
        })

# Report mismatches
if mismatches:
    print("Mismatched Offsets:")
    for mismatch in mismatches:
        print(
            "Topic: %s, Partition: %s, File1 Offset: %s, File2 Offset: %s, Difference: %s" % (
                mismatch["topic"],
                mismatch["partition"],
                mismatch["file1_offset"],
                mismatch["file2_offset"],
                mismatch["difference"]
            )
        )
else:
    print("