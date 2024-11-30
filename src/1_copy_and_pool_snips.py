import csv, os, shutil, sys
from pathlib import Path
from collections import Counter
from common import load_config

def find_snips_directories(input_dir):
    snips_dirs = []
    for root, dirs, _ in os.walk(input_dir):
        for dir_name in dirs:
            if dir_name.lower() == 'snips':
                full_path = os.path.join(root, dir_name)
                # Normalize the path to remove any trailing slashes
                full_path = os.path.normpath(full_path)
                snips_dirs.append(full_path)
    #print(f"Found 'snips' directories: {snips_dirs}")  # Debugging line
    return snips_dirs

def perform_sanity_checks(snips_dirs):
    """
    Perform sanity checks:
    1. Ensure no duplicate folder names among parent directories of 'snips'.
    2. Validate the contents of 'mewc_out.csv' in the first snips directory.
    """
    # Gather parent folder names (one level up from 'snips')
    parent_folders = []
    for snips_dir in snips_dirs:
        snips_path = Path(snips_dir).resolve()
        parent_dir = snips_path.parent
        parent_name = parent_dir.name
        parent_folders.append(parent_name)

    # Check for duplicate folder names
    duplicates = [name for name, count in Counter(parent_folders).items() if count > 1]
    if duplicates:
        print("Aborted: Every folder that contains camera-trap files must be uniquely named!")
        print("Advice is to use some system that gives all relevant information for the camera site, e.g., rg_s2_c3 where:")
        print("  rg = region, s = site number, and c = camera number.")
        print("Whatever system you use, unique camera-folder names is vital to identify data provenance in the consolidated species table.")
        print(f"Duplicate folder names found: {', '.join(duplicates)}")
        sys.exit(1)

    # Proceed with the existing checks for the first snips directory
    first_snips_dir = snips_dirs[0]
    camera_folder = Path(first_snips_dir).resolve().parent
    mewc_csv_path = camera_folder / 'mewc_out.csv'

    # Check 1: Confirm 'mewc_out.csv' exists
    if not mewc_csv_path.is_file():
        print("Aborted: You must run mewc-service.ps1 before proceeding!")
        sys.exit(1)
    else:
        print(f"Found 'mewc_out.csv' in '{camera_folder}'. Proceeding with sanity checks.")

    # Check 2a: Validate filename and rand_name
    try:
        with mewc_csv_path.open('r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            first_row = next(reader, None)
            if first_row is None:
                print("Aborted: 'mewc_out.csv' is empty.")
                sys.exit(1)

            filename = first_row.get('filename', '')
            rand_name = first_row.get('rand_name', '')

            if len(filename) < 8 or len(rand_name) < 8:
                print("Aborted: 'filename' or 'rand_name' in 'mewc_out.csv' is too short.")
                sys.exit(1)

            if filename[:8] == rand_name[:8]:
                print("Aborted: Snips must have been set to be renamed when you ran mewc-service.ps1 (check its parameter settings and redo).")
                sys.exit(1)
            else:
                print("Filename and Rand Name check passed.")

    except Exception as e:
        print(f"Error reading 'mewc_out.csv': {e}")
        sys.exit(1)

    # Check 2b: Validate class_rank
    try:
        with mewc_csv_path.open('r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            class_ranks = []
            for row in reader:
                try:
                    class_rank = float(row.get('class_rank', 0))
                    class_ranks.append(class_rank)
                except ValueError:
                    print("Aborted: Invalid 'class_rank' value in 'mewc_out.csv'.")
                    sys.exit(1)

            if not class_ranks:
                print("Aborted: 'mewc_out.csv' contains no data rows.")
                sys.exit(1)

            max_class_rank = max(class_ranks)
            if max_class_rank > 1:
                print("Aborted: You must set parameter settings for mewc-service.ps1 to only include top class; fix this and redo.")
                sys.exit(1)
            else:
                print("Class Rank check passed.")
    except Exception as e:
        print(f"Error processing 'class_rank' in 'mewc_out.csv': {e}")
        sys.exit(1)

    print("All sanity checks passed.\n")

def copy_files(snips_dirs, output_dir, service_dir):
    if not os.path.exists(output_dir):
        print(f"Output directory '{output_dir}' does not exist. Creating it...")
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created output directory '{output_dir}'.\n")

    if not snips_dirs:
        print(f"No 'snips' directories found in '{service_dir}'. Nothing to copy.")
        return

    print(f"Found {len(snips_dirs)} 'snips' director{'y' if len(snips_dirs) == 1 else 'ies'}.\n")

    for snips_dir in snips_dirs:
        try:
            files = [f for f in os.listdir(snips_dir) if os.path.isfile(os.path.join(snips_dir, f))]
        except Exception as e:
            print(f"Error accessing files in '{snips_dir}': {e}\n")
            continue

        if not files:
            print(f"No files found in '{snips_dir}'. Skipping.\n")
            continue

        for file_name in files:
            source_path = os.path.join(snips_dir, file_name)
            destination_path = os.path.join(output_dir, file_name)

            try:
                shutil.copy2(source_path, destination_path)
                #print(f"Copied '{file_name}' to '{output_dir}'.")
            except shutil.SameFileError:
                print(f"Source and destination represent the same file: '{file_name}'. Skipping.")
            except PermissionError:
                print(f"Permission denied while copying '{file_name}'. Skipping.")
            except Exception as e:
                print(f"Failed to copy '{file_name}': {e}")

        print(f"Completed processing folder: {snips_dir}\n")

    print("All processing complete.")

def main():
    config = load_config()

    # Extract necessary paths from the config
    service_directory = config.get("service_directory")
    snip_pool = config.get("snip_pool")

    if not service_directory or not snip_pool:
        print("Configuration file is missing required fields: 'service_directory' and/or 'snip_pool'.")
        sys.exit(1)

    if not os.path.isdir(service_directory):
        print(f"Input directory '{service_directory}' does not exist or is not a directory.")
        sys.exit(1)

    # Find all 'snips' directories
    snips_dirs = find_snips_directories(service_directory)

    if not snips_dirs:
        print(f"No 'snips' directories found in '{service_directory}'. Nothing to do.")
        sys.exit(0)

    # Perform sanity checks on the 'snips' directories
    perform_sanity_checks(snips_dirs)

    # Proceed to copy files
    copy_files(snips_dirs, snip_pool, service_directory)

if __name__ == "__main__":
    main()
