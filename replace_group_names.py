import os
import re
import sys
# Define the old and new group names for replacement
OLD_GROUP = "velero.io"
NEW_GROUP = "cloudcasa.io"
# Directories to scan for files that need updating (relative to the Velero directory path provided)
BASE_DIRS = ["pkg/apis/velero", "pkg/controller"]
# Compile regular expressions to match patterns that need changing
# 1. Group definition in Go code (e.g., Group: "velero.io")
GROUP_VERSION_PATTERN = re.compile(r'Group:\s*"%s"' % OLD_GROUP)
# 2. Group name annotation in the doc.go files (e.g., +groupName=velero.io)
GROUP_NAME_PATTERN = re.compile(r'\+groupName=%s' % OLD_GROUP)
# 3. kubebuilder RBAC annotations for resources and statuses (e.g., kubebuilder:rbac:groups=velero.io)
RBAC_PATTERN = re.compile(r'kubebuilder:rbac:groups=%s' % OLD_GROUP)
# Function to replace group names in a file and log the changes
def replace_group_name_in_file(file_path):
    """
    This function reads a file, replaces occurrences of the old group name (velero.io)
    with the new group name (cloudcasa.io) in both code and annotations, and writes
    the updated content back to the file. It also logs the changes made to each file.
    :param file_path: Path to the file where replacements need to be made.
    """
    # Read the file content
    with open(file_path, 'r') as file:
        content = file.read()
    # Track changes made
    changes_made = False
    # Replace occurrences of the group name in code (e.g., Group: "velero.io")
    new_content = GROUP_VERSION_PATTERN.sub('Group: "%s"' % NEW_GROUP, content)
    if new_content != content:
        print(f"=> Updating Group name in code for {file_path}: Replaced 'Group: \"{OLD_GROUP}\"' with 'Group: \"{NEW_GROUP}\"'")
        content = new_content
        changes_made = True
    # Replace the group name in the doc.go annotations (e.g., +groupName=velero.io)
    new_content = GROUP_NAME_PATTERN.sub('+groupName=%s' % NEW_GROUP, content)
    if new_content != content:
        print(f"=> Updating +groupName annotation for {file_path}: Replaced '+groupName={OLD_GROUP}' with '+groupName={NEW_GROUP}'")
        content = new_content
        changes_made = True
    # Replace the group name in the kubebuilder RBAC annotations (e.g., kubebuilder:rbac:groups=velero.io)
    new_content = RBAC_PATTERN.sub('kubebuilder:rbac:groups=%s' % NEW_GROUP, content)
    if new_content != content:
        print(f"=> Updating kubebuilder RBAC for {file_path}: Replaced 'kubebuilder:rbac:groups={OLD_GROUP}' with 'kubebuilder:rbac:groups={NEW_GROUP}'")
        content = new_content
        changes_made = True
    # If changes were made, overwrite the file with the updated content
    if changes_made:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"*** ==> Changes applied to file: {file_path}")
    else:
        print(f"*** ==> No changes required for file: {file_path}")
# Function to traverse directories and process files
def process_directory(directory):
    """
    This function recursively traverses a directory, and for each .go file found,
    it applies the group name replacement logic by calling the replace_group_name_in_file function.
    :param directory: Base directory to start the recursive traversal.
    """
    # Traverse the directory and its subdirectories
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            # Process only Go files (.go)
            if filename.endswith(".go"):
                file_path = os.path.join(dirpath, filename)
                # Apply the group name replacement logic on the file
                replace_group_name_in_file(file_path)
if __name__ == "__main__":
    # Check if the user provided a Velero directory path as a command-line argument
    if len(sys.argv) != 2:
        print("Usage: python3 replace_group_names.py <path_to_velero_directory>")
        sys.exit(1)
    # Get the Velero directory path from the first command-line argument
    velero_dir = sys.argv[1]
    # Check if the provided directory exists
    if not os.path.isdir(velero_dir):
        print(f"Error: The directory {velero_dir} does not exist.")
        sys.exit(1)
    # Loop through the base directories we want to process (relative to the Velero directory)
    for base_dir in BASE_DIRS:
        full_path = os.path.join(velero_dir, base_dir)
        if os.path.isdir(full_path):
            print(f"=> Processing directory: {full_path}")
            process_directory(full_path)
        else:
            print(f"Warning: Directory {full_path} does not exist. Skipping...")
    print(" ===========================================")
    print(" **** Group name replacement completed. ****")
    print(" ===========================================")