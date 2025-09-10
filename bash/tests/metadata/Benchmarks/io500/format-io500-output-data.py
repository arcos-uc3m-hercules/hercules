import sys
import os

def format_io500_data(data):
    """
    Parses IO500 benchmark data and formats it as a CSV string
    for easy use in spreadsheet software like Excel.
    
    Args:
        data (str): A multi-line string containing the benchmark results.
    
    Returns:
        str: A formatted CSV string.

    Example:
        python3 format-io500-output-data.py ./results-hercules-full/2025.08.21-19.24.16/result_summary.txt
    """
    lines = data.strip().split('\n')
    
    # Initialize a list to hold the formatted rows.
    formatted_rows = []
    
    # Add the header row for the CSV.
    formatted_rows.append("Test,Value,Unit,Time (s)")
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines or lines without relevant markers.
        if not line or not (line.startswith('[RESULT]') or line.startswith('[SCORE ]') or line.startswith('[      ]')):
            continue
            
        # The line might start with a leading non-breaking space, so we check again.
        if not line.startswith('[RESULT]') and not line.startswith('[SCORE ]') and not line.startswith('[      ]'):
            continue

        # Handle the standard RESULT lines.
        if line.startswith('[RESULT]'):
            try:
                result_part, time_part = line.split(' : time ')
                result_parts = result_part.split()
                
                value = result_parts[-2]
                unit = result_parts[-1]
                test_name = ' '.join(result_parts[1:-2])
                time = time_part.split()[0]

                row = f'"{test_name}",{value},{unit},{time}'
                formatted_rows.append(row)
            except (ValueError, IndexError):
                print(f"Skipping malformed RESULT line: {line}", file=sys.stderr)
                continue

        # Handle lines with empty brackets.
        elif line.startswith('[      ]'):
            try:
                result_part, time_part = line.split(' : time ')
                result_parts = result_part.split()
                
                # The structure is slightly different for these lines
                value = result_parts[-2]
                unit = result_parts[-1]
                # The test name starts after the [      ] part
                test_name = ' '.join(result_parts[2:-2])
                time = time_part.split()[0]
                
                row = f'"{test_name}",{value},{unit},{time}'
                formatted_rows.append(row)
            except (ValueError, IndexError):
                print(f"Skipping malformed [      ] line: {line}", file=sys.stderr)
                continue

        # Handle the special SCORE line.
        elif line.startswith('[SCORE ]'):
            try:
                score_parts = line.split(' : ')
                
                bw_part = score_parts[0].split()
                bandwidth_value = bw_part[-2]
                bandwidth_unit = bw_part[-1]
                formatted_rows.append(f'"Bandwidth",{bandwidth_value},{bandwidth_unit},')
                
                iops_part = score_parts[1].split()
                iops_value = iops_part[-2]
                iops_unit = iops_part[-1]
                formatted_rows.append(f'"IOPS",{iops_value},{iops_unit},')
                
                total_part = score_parts[2].split()
                total_value = total_part[-1]
                formatted_rows.append(f'"TOTAL",{total_value},,')
            except (ValueError, IndexError):
                print(f"Skipping malformed SCORE line: {line}", file=sys.stderr)
                continue

    return "\n".join(formatted_rows)

def main():
    """
    Main function to read a file, process its content, and print the output.
    """
    if len(sys.argv) < 2:
        print("Error: No input file specified.", file=sys.stderr)
        print(f"Usage: python {os.path.basename(sys.argv[0])} <input_file>", file=sys.stderr)
        sys.exit(1)
        
    filename = sys.argv[1]

    try:
        with open(filename, 'r') as f:
            raw_data = f.read()
            csv_output = format_io500_data(raw_data)
            print(csv_output)
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

