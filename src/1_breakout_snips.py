import os, shutil
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from collections import Counter
from common import load_config, SanityCheckError

def find_mewc_out_files(service_directory, mewc_filename):
    """Find all mewc files in the service directory using specified filename."""
    mewc_files = list(Path(service_directory).rglob(mewc_filename))
    return mewc_files

def perform_sanity_checks(mewc_files):
    """Check for duplicate camera site folder names."""
    camera_sites = []
    for mewc_file in mewc_files:
        camera_site = mewc_file.parent.resolve()
        camera_sites.append(camera_site.name)

    duplicates = [item for item, count in Counter(camera_sites).items() if count > 1]
    if duplicates:
        print("Aborted: Duplicate camera site folder names found.")
        print(f"Duplicate folder names: {', '.join(duplicates)}")
        raise SanityCheckError()
    else:
        print("All camera site folder names are unique.")

def create_species_breakout(mewc_files, classified_snips_path, probability_bins):
    """
    Create species breakout by moving snips into species/probability folders.
    If probability_bins is empty, moves snips directly into species folders.
    """
    os.makedirs(classified_snips_path, exist_ok=True)

    def determine_prob_folder(prob):
        """Determine the probability folder based on the prob value."""
        if not probability_bins:
            return None
        for bin_label in probability_bins:
            threshold = bin_label / 100
            if prob >= threshold:
                return str(bin_label)
        return str(probability_bins[-1])

    for mewc_file in tqdm(mewc_files, desc="Processing mewc_out.csv files"):
        camera_site = mewc_file.parent.resolve()
        snips_dir = camera_site / 'snips'
        if not snips_dir.is_dir():
            print(f"Warning: 'snips' directory not found in {camera_site}")
            continue
            
        mewc_df = pd.read_csv(mewc_file)
        for _, row in mewc_df.iterrows():
            rand_name = row['rand_name']
            class_name = row['class_name']
            prob = row['prob']
            
            src_file = snips_dir / rand_name
            if not src_file.is_file():
                print(f"Warning: Snip file {src_file} not found.")
                continue
                
            # Create destination directory based on whether probability binning is used
            prob_folder = determine_prob_folder(prob)
            dest_dir = (Path(classified_snips_path) / class_name / prob_folder 
                       if prob_folder else Path(classified_snips_path) / class_name)
            os.makedirs(dest_dir, exist_ok=True)
            dest_file = dest_dir / rand_name
            shutil.copy2(src_file, dest_file)
            
    print("Completed classifying and moving all snips.")

def main():
    config = load_config()
    service_directory = config.get('service_directory')
    classified_snips_path = config.get('classified_snips_path')
    probability_bins = config.get('probability_bins', [99, 90, 50, 20])
    mewc_filename = config.get('mewc_filename', 'mewc_out.csv')

    if not service_directory or not classified_snips_path:
        print("Error: 'service_directory' and/or 'classified_snips_path' is missing in configuration.")
        raise SanityCheckError()

    mewc_files = find_mewc_out_files(service_directory, mewc_filename)

    if not mewc_files:
        print(f"No '{mewc_filename}' files found in the service directory.")
        raise SanityCheckError()

    perform_sanity_checks(mewc_files)
    create_species_breakout(mewc_files, classified_snips_path, probability_bins)

    print("All processing completed successfully.")

if __name__ == "__main__":
    main()
