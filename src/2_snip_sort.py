import subprocess, sys, os, shutil
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from common import load_config

def run_powershell_script(ps_params):
    """Run the mewc_predict PowerShell script with parameters."""
    script_path = ps_params.get('script_path')
    if not script_path or not Path(script_path).is_file():
        print(f"Error: PowerShell script not found at '{script_path}'.")
        sys.exit(1)

    # Construct the PowerShell command
    command = [
        'powershell.exe',
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', script_path,
        '-i', ps_params.get('input_snips'),
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
    # Assuming probability_bins are in descending order
    for bin_label in probability_bins:
        threshold = bin_label / 100  # Convert to 0-1 range
        if prob >= threshold:
            return str(bin_label)
    return str(probability_bins[-1])  # Assign to the lowest bin if none match

def classify_and_move_files(params):
    """Classify snips based on probability and move them accordingly."""
    csv_path = params.get('snip_pool_csv')
    snip_pool = params.get('snip_pool')
    classified_snips_path = params.get('classified_snips_path')
    probability_bins = params.get('probability_bins', [])

    # Validate required parameters
    if not all([csv_path, snip_pool, classified_snips_path]):
        print("Error: One or more required parameters are missing in 'params.yaml'. Aborting.")
        sys.exit(1)

    # Read the CSV file
    try:
        data = pd.read_csv(csv_path)
        print(f"Successfully read CSV file '{csv_path}'.")
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_path}' not found. Aborting.")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_path}' is empty. Aborting.")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file '{csv_path}': {e}. Aborting.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Unexpected error reading CSV file '{csv_path}': {e}. Aborting.")
        sys.exit(1)

    # Initialize progress bar
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

        # Determine destination folder
        if probability_bins:
            prob_folder = determine_prob_folder(prob, probability_bins)
            destination_path = os.path.join(classified_snips_path, class_name, prob_folder)
        else:
            # No probability bins, move to class_name folder directly
            destination_path = os.path.join(classified_snips_path, class_name)

        try:
            # Ensure the destination directory exists
            os.makedirs(destination_path, exist_ok=True)

            # Define source and destination file paths
            source_file_path = os.path.join(snip_pool, filename)
            destination_file_path = os.path.join(destination_path, filename)

            # Check if the source file exists
            if not os.path.isfile(source_file_path):
                print(f"Warning: Source file '{source_file_path}' does not exist. Skipping.")
                progress_bar.update(1)
                continue

            # Move the file
            shutil.move(source_file_path, destination_file_path)
            #print(f"Moved '{filename}' to '{destination_path}'.")
        except Exception as e:
            print(f"Error: Failed to move '{filename}': {e}")
        finally:
            progress_bar.update(1)

    progress_bar.close()
    print("Completed classifying and moving all snips.")

def main():
    config = load_config()

    script = config.get('mewc_predict_powershell', {})
    if not script:
        print("Configuration file is missing required fields: 'mewc_predict_powershell'.")
        sys.exit(1)

    # Run the PowerShell script
    run_powershell_script(script)

    # Classify and move the snips based on the AI classification results
    classify_and_move_files(config)

if __name__ == "__main__":
    main()
