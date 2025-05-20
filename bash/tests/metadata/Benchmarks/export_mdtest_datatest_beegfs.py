import csv
import re
import os
from datetime import datetime

def parse_file(file_path):
    data = {
        "LogID":"",
        "Path": "",
        "FS": "",
        "Used FS": "",
        "Inodes": "",
        "Used Inodes (%)": "",
        "Tasks": "",
        "Metadata servers": "",
        "Data servers": "",
        "Policy": "",
        "Block size": "",
        "Files/Directories": "",
        "Number of Nodes": "", 
        "Directory creation": "",
        "Directory stat": "",
        "Directory rename": "",
        "Directory removal": "",
        "File creation": "",
        "File stat": "",
        "File read": "",
        "File removal": "",
        "Tree creation": "",
        "Tree removal": "",
        "Timestamp": ""
    }

    data["LogID"] = file_path

    with open(file_path, 'r') as file:
        content = file.read()

    # Extract Path
    path_match = re.search(r"Path\s*:\s*(.+)", content)
    if path_match:
        data["Path"] = path_match.group(1).strip()

    # Extract FS and Used FS
    fs_match = re.search(r"FS\s*:\s*([\d.]+)\s*GiB\s*Used FS:\s*([\d.-]+)%", content)
    if fs_match:
        data["FS"] = fs_match.group(1).strip()
        data["Used FS"] = fs_match.group(2).strip()

    # Extract Inodes and Used Inodes
    inodes_match = re.search(r"Inodes:\s*([\d.]+)\s*Mi\s*Used Inodes:\s*([\d.]+)%", content)
    if inodes_match:
        data["Inodes"] = inodes_match.group(1).strip()
        data["Used Inodes"] = inodes_match.group(2).strip()

    # Extract Tasks and Files/Directories
    tasks_match = re.search(r"(\d+)\s*tasks,\s*(\d+)\s*files/directories", content)
    if tasks_match:
        data["Tasks"] = tasks_match.group(1).strip()
        data["Files/Directories"] = tasks_match.group(2).strip()

    metadata_servers_match = re.search(r"METADATA SERVERS (\d+)", content)
    # print("metadata_servers: ", metadata_servers_match.group(1).strip())
    if metadata_servers_match:
        data["Metadata servers"] = metadata_servers_match.group(1).strip()
    else:
        data["Metadata servers"] = "N/A"  # Default value if not found   

    data_servers_match = re.search(r"DATA SERVERS (\d+)", content)
    # print("data_servers: ", data_servers_match.group(1).strip())
    if data_servers_match:
        data["Data servers"] = data_servers_match.group(1).strip()
    else:
        data["Data servers"] = "N/A"  # Default value if not found        

    block_size_match = re.search(r"HERCULES_BLOCK_SIZE - (\d+)", content)
    # print("block_size: ", block_size_match.group(1).strip())
    if block_size_match:
        data["Block size"] = block_size_match.group(1).strip()
    else:
        data["Block size"] = "N/A"  # Default value if not found       

    policy_match = re.search(r"Starting Hercules with (..) policy", content)
    # print("policy: ", policy_match.group(1).strip())
    if policy_match:
        data["Policy"] = policy_match.group(1).strip()
    else:
        data["Policy"] = "N/A"  # Default value if not found    

    # Extract Number of Nodes
    nodes_match = re.search(r"was launched with \d+ total task\(s\) on (\d+) node\(s\)", content)
    if nodes_match:
        data["Number of Nodes"] = nodes_match.group(1).strip()
    else:
        data["Number of Nodes"] = "N/A"  # Default value if not found

    # Extract Operation Rates
    operations = [
        "Directory creation", "Directory stat", "Directory rename", "Directory removal",
        "File creation", "File stat", "File read", "File removal", "Tree creation", "Tree removal"
    ]
    for op in operations:
        op_match = re.search(rf"{op}\s*([\d.]+)\s*([\d.]+)\s*([\d.]+)\s*([\d.]+)", content)
        if op_match:
            data[op] = op_match.group(3).strip()  # Mean value

    # Extract Timestamp
    timestamp_match = re.search(r"-- finished at (\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) --", content)
    if timestamp_match:
        timestamp_str = timestamp_match.group(1).strip()
        data["Timestamp"] = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

    return data

def export_to_csv(data_list, output_csv):
    if not data_list:
        print("No data to export.")
        return

    fieldnames = data_list[0].keys()
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in data_list:
            writer.writerow(data)

def main(file_paths, output_csv):
    data_list = []
    for file_path in file_paths:
        data = parse_file(file_path)
        data_list.append(data)

    export_to_csv(data_list, output_csv)
    print(f"Data exported to {output_csv}")

if __name__ == "__main__":
    # List of file paths to parse
    # Base directory where the log files are located
    base_path = "logs/lustre/" 

    # Dynamically get all .log files in the directory
    file_paths = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.endswith("_mdtest.log")]

    file_paths.sort(key=os.path.getmtime)  # Sort by modification time

    print(file_paths)
    
    output_csv = f"mdtest_{fs}.csv"  # Output CSV file name

    main(file_paths, output_csv)
