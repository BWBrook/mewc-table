import os
import sys
import json
import piexif
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def load_config():
    """
    Load configuration parameters from params.json.
    """
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / 'params.json'
    if not config_path.is_file():
        print(f"Configuration file '{config_path}' does not exist.")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file '{config_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error reading '{config_path}': {e}")
        sys.exit(1)

def load_dataframe(output_table_path):
    """
    Load the consolidated species table as a pandas DataFrame.
    """
    csv_path = Path(str(output_table_path) + ".csv")
    pkl_path = Path(str(output_table_path) + ".pkl")

    if csv_path.exists():
        return pd.read_csv(csv_path)
    elif pkl_path.exists():
        return pd.read_pickle(pkl_path)
    else:
        raise FileNotFoundError("No valid .csv or .pkl file found for output_table.")

def save_dataframe(df, output_table_path):
    """
    Save the updated DataFrame as both CSV and Pickle files.
    """
    csv_path = output_table_path.with_suffix(".csv")
    pkl_path = output_table_path.with_suffix(".pkl")
    df.to_csv(csv_path, index=False)
    df.to_pickle(pkl_path)
    print(f"Updated table saved to {csv_path} and {pkl_path}.")

def extract_flash_fired(filepath):
    """
    Extract whether the flash fired from EXIF data of the image.
    Return 1 if flash fired, 0 otherwise.
    """
    try:
        exif_data = piexif.load(str(filepath))
        if 37385 in exif_data["Exif"]:
            flash_status = exif_data["Exif"][37385]
            return 1 if flash_status != 0 else 0
    except Exception as e:
        print(f"Error reading EXIF from {filepath}: {e}")
    return 0  # Default to no flash

def create_base_filename(filename):
    """
    Strip -n suffix from the filename.
    Example: 'I__00001-0.JPG' -> 'I__00001.JPG'
    """
    parts = filename.split('.')
    if len(parts) < 2:
        return filename
    name, ext = '.'.join(parts[:-1]), parts[-1]
    if '-' in name:
        return name.rsplit('-', 1)[0] + '.' + ext
    return filename

def update_flash_fired(service_directory, output_table_path):
    """
    Update the output table with a 'flash_fired' column for all images in \animal folders.
    """
    print("Updating flash_fired data...")
    
    # Load the output table
    output_table = load_dataframe(output_table_path)
    
    # Initialize flash_fired column
    output_table['flash_fired'] = -1  # Default to -1 for rows not matched to any image
    
    # Iterate over camera sites
    service_path = Path(service_directory)
    animal_dirs = service_path.rglob("animal")

    for animal_dir in tqdm(animal_dirs, desc="Processing animal folders"):
        camera_site = animal_dir.parent.name  # Extract camera_site from folder structure
        for image_path in animal_dir.rglob("*.JPG"):  # Recursively search for images
            # Normalize the filename by stripping the suffix
            base_filename = create_base_filename(image_path.name)
            flash_fired_value = extract_flash_fired(image_path)

            # Update the table for the matching filename and camera_site
            # Normalize 'filename' in output_table for comparison
            mask = (output_table['filename'].apply(create_base_filename) == base_filename) & \
                   (output_table['camera_site'] == camera_site)
            output_table.loc[mask, 'flash_fired'] = flash_fired_value

    print("Flash data updated for all matching rows.")
    return output_table

def main():
    # Load configuration
    config = load_config()
    service_directory = config.get("service_directory")
    output_table = config.get("output_table")

    if not service_directory or not output_table:
        print("Configuration file is missing required fields: 'service_directory' and/or 'output_table'.")
        sys.exit(1)

    output_table_path = Path(output_table)

    # Update the output table with flash_fired data
    updated_table = update_flash_fired(service_directory, output_table_path)

    # Save the updated table
    save_dataframe(updated_table, output_table_path)

    print("Flash fired data successfully added to the output table.")

if __name__ == "__main__":
    main()
