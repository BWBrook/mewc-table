import csv, os, shutil, subprocess, sys
import pandas as pd
from pathlib import Path
from collections import Counter
from tqdm import tqdm
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

    print("All sanity checks passed.")

def copy_files(snips_dirs, output_dir, service_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    if not snips_dirs:
        print(f"No 'snips' directories found in '{service_dir}'. Nothing to copy.")
        return

    print(f"Found {len(snips_dirs)} 'snips' director{'y' if len(snips_dirs) == 1 else 'ies'}.")

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

        print(f"Processing: {snips_dir}")

def run_powershell_script(ps_params, snip_pool_path):
    """Run the mewc_predict PowerShell script with parameters."""
    script_path = ps_params.get('script_path')
    if not script_path or not Path(script_path).is_file():
        print(f"Error: PowerShell script not found at '{script_path}'.")
        sys.exit(1)

    # Use parent of snip_pool_path as input_snips
    input_snips = str(Path(snip_pool_path).parent)

    command = [
        'powershell.exe',
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', script_path,
        '-i', input_snips,  # Use derived path
        '-p', ps_params.get('predict_env'),
        '-c', ps_params.get('class_map'),
        '-m', ps_params.get('model_file'),
        '-g', ps_params.get('gpu_id')
    ]

    print(f"Running PowerShell script: {script_path}")
    try:
        result = subprocess.run(command, check=True, text=True)
        print(f"Successfully ran PowerShell script: {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error: PowerShell script '{script_path}' failed with message:\n{e.stderr}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Unexpected error while running PowerShell script '{script_path}': {e}")
        sys.exit(1)

def determine_prob_folder(prob, probability_bins):
    """Determine the probability folder based on the 'prob' value."""
    for bin_label in probability_bins:
        threshold = bin_label / 100
        if prob >= threshold:
            return str(bin_label)
    return str(probability_bins[-1])

def setup_classified_path(config):
    """Create classified_snips_path directory and move MEWC output files."""
    classified_path = config.get('classified_snips_path')
    if not classified_path:
        print("Error: 'classified_snips_path' is missing in configuration.")
        sys.exit(1)
    
    # Create classified_snips_path
    os.makedirs(classified_path, exist_ok=True)
    
    # Define file extensions to move
    extensions = ['.csv', '.pkl']
    base_filename = 'mewc_out'
    
    # Move both CSV and PKL files
    for ext in extensions:
        source_file = Path(config['snip_pool']).parent / f'{base_filename}{ext}'
        dest_file = Path(classified_path) / f'{base_filename}{ext}'
        
        if source_file.is_file():
            try:
                shutil.move(str(source_file), str(dest_file))
                print(f"Moved {source_file.name} to classified directory")
            except Exception as e:
                print(f"Error moving {source_file.name}: {e}")
                sys.exit(1)
        else:
            print(f"Warning: {source_file.name} not found")

def classify_and_move_files(params):
    """Classify snips based on probability and move them accordingly."""
    csv_path = params.get('snip_pool_csv')
    snip_pool = params.get('snip_pool')
    classified_snips_path = params.get('classified_snips_path')
    probability_bins = params.get('probability_bins', [])

    if not all([csv_path, snip_pool, classified_snips_path]):
        print("Error: One or more required parameters are missing in 'params.yaml'. Aborting.")
        sys.exit(1)

    try:
        data = pd.read_csv(csv_path)
        print(f"Successfully read CSV file '{csv_path}'.")
    except Exception as e:
        print(f"Error reading CSV file '{csv_path}': {e}")
        sys.exit(1)

    print("Starting to classify and move snips...")
    progress_bar = tqdm(total=len(data), desc="Processing snips", unit="snip")

    for _, row in data.iterrows():
        prob = row.get('prob', 0)
        class_name = row.get('class_name', 'unknown')
        filename = row.get('filename', '')

        if pd.isna(filename) or not filename:
            print("Warning: Encountered a row with missing 'filename'. Skipping.")
            progress_bar.update(1)
            continue

        if probability_bins:
            prob_folder = determine_prob_folder(prob, probability_bins)
            destination_path = os.path.join(classified_snips_path, class_name, prob_folder)
        else:
            destination_path = os.path.join(classified_snips_path, class_name)

        try:
            os.makedirs(destination_path, exist_ok=True)
            source_file_path = os.path.join(snip_pool, filename)
            destination_file_path = os.path.join(destination_path, filename)

            if not os.path.isfile(source_file_path):
                print(f"Warning: Source file '{source_file_path}' does not exist. Skipping.")
                progress_bar.update(1)
                continue

            shutil.move(source_file_path, destination_file_path)
        except Exception as e:
            print(f"Error: Failed to move '{filename}': {e}")
        finally:
            progress_bar.update(1)

    progress_bar.close()
    print("Completed classifying and moving all snips.")

def get_derived_paths(config):
    """
    Derive snip_pool and csv paths from classified_snips_path.
    Returns tuple of (snip_pool_path, snip_pool_csv_path).
    """
    classified_path = config.get('classified_snips_path')
    if not classified_path:
        print("Error: 'classified_snips_path' is missing in configuration.")
        sys.exit(1)
    
    # Get base directory from classified_snips_path
    base_dir = str(Path(classified_path).parent)
    
    # Construct derived paths
    snip_pool_path = str(Path(base_dir) / 'snip_pool' / 'snips')
    snip_pool_csv_path = str(Path(classified_path) / 'mewc_out.csv')
    
    return snip_pool_path, snip_pool_csv_path

def cleanup_snip_pool(snip_pool_dir):
    """Remove snip_pool directory if empty after processing."""
    try:
        if os.path.exists(snip_pool_dir):
            # Remove snips subdirectory
            snips_path = Path(snip_pool_dir)
            if snips_path.is_dir() and not any(snips_path.iterdir()):
                snips_path.rmdir()
            
            # Remove parent snip_pool directory if empty
            parent_dir = snips_path.parent
            if parent_dir.is_dir() and not any(parent_dir.iterdir()):
                parent_dir.rmdir()
                print(f"Removed empty directory: {parent_dir}")
    except Exception as e:
        print(f"Warning: Failed to cleanup snip_pool directory: {e}")

def main():
    """Execute the complete workflow for processing camera trap snips."""
    config = load_config()

    # Extract configuration
    service_directory = config.get("service_directory")
    ps_config = config.get('mewc_predict_powershell', {})

    # Get derived paths
    snip_pool, snip_pool_csv = get_derived_paths(config)
    
    # Update config with derived paths
    config['snip_pool'] = snip_pool
    config['snip_pool_csv'] = snip_pool_csv
    ps_config['input_snips'] = str(Path(snip_pool).parent)

    if not service_directory:
        print("Configuration file is missing 'service_directory'.")
        sys.exit(1)

    if not os.path.isdir(service_directory):
        print(f"Input directory '{service_directory}' does not exist.")
        sys.exit(1)

    # Phase 1: Find and validate snips directories
    print("\nPhase 1: Locating and validating snips directories...")
    snips_dirs = find_snips_directories(service_directory)
    if not snips_dirs:
        print(f"No 'snips' directories found in '{service_directory}'. Nothing to do.")
        sys.exit(0)
    perform_sanity_checks(snips_dirs)

    # Phase 2: Copy files to snip pool
    print("\nPhase 2: Copying files to temporary snip pool...")
    copy_files(snips_dirs, snip_pool, service_directory)

    # Phase 3: Run MEWC prediction
    print("\nPhase 3: Running MEWC classifier prediction...")
    run_powershell_script(ps_config, snip_pool)
    
    # Setup Phase: Prepare classified_snips_path
    print("\nSetting up classified snips directory...")
    setup_classified_path(config)

    # Phase 4: Classify and move files
    print("\nPhase 4: Classifying and breaking out snip files...")
    classify_and_move_files(config)

    # Phase 5: Cleanup
    print("\nPhase 5: Cleaning up temporary directories...")
    cleanup_snip_pool(snip_pool)

    print("\nAll processing phases completed successfully.")

if __name__ == "__main__":
    main()
