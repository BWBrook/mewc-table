import shutil, sys
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from common import load_config

def load_dataframe(file_path):
    """
    Load the mewc_species-site_id CSV or pickle file into a pandas DataFrame.
    """
    file_path = Path(file_path)  # Ensure it's a Path object
    if file_path.suffix == '.csv':
        df = pd.read_csv(file_path)
    elif file_path.suffix in ['.pkl', '.pickle']:
        df = pd.read_pickle(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or pickle file.")
    return df

def create_base_filename(filename):
    """
    Create base filename by removing suffix like -0, -1, etc.
    Example: 'I__00001-0.JPG' -> 'I__00001.JPG'
    """
    parts = filename.split('.')
    if len(parts) < 2:
        return filename  # No extension, return as is
    name, ext = '.'.join(parts[:-1]), parts[-1]
    if '-' in name:
        base_name = name.rsplit('-', 1)[0] + '.' + ext
    else:
        base_name = filename
    return base_name

def prepare_mapping(df):
    """
    Prepare a mapping from (camera_site, base_filename) to class_name.
    """
    # Create base_filename column
    df['base_filename'] = df['filename'].apply(create_base_filename)
    
    # Sort by prob descending to get highest prob first (if duplicates exist)
    df_sorted = df.sort_values(['camera_site', 'base_filename', 'prob'], ascending=[True, True, False])
    
    # Drop duplicates to keep the row with highest prob for each (camera_site, base_filename)
    mapping_df = df_sorted.drop_duplicates(subset=['camera_site', 'base_filename'], keep='first')
    
    # Create a dictionary for quick lookup
    mapping = {}
    for _, row in mapping_df.iterrows():
        key = (row['camera_site'], row['base_filename'])
        mapping[key] = row['class_name']  # Use correct column name here
    return mapping

def process_animal_directories(service_base_dir, mapping):
    """
    Recursively process all \animal directories in the service folders.
    Create class_name subdirectories and move .JPG files accordingly.
    Files without a mapping will be moved into an 'other_object' folder.
    """
    service_base = Path(service_base_dir)
    
    # Recursively find all 'animal' directories
    animal_dirs = [animal_dir for animal_dir in service_base.rglob('animal') if animal_dir.is_dir()]
    
    if not animal_dirs:
        print("No 'animal' directories found. Ensure the directory structure is correct.")
        return
    
    # Iterate through all 'animal' directories
    for animal_dir in tqdm(animal_dirs, desc="Processing animal directories"):
        # Get camera_site from the parent folder of the 'animal' directory
        camera_site = animal_dir.parent.name
        print(f"\nProcessing 'animal' directory for camera_site: {camera_site}")
        
        # Get unique class_name values for this camera_site
        relevant_keys = [key for key in mapping if key[0] == camera_site]
        classes = {mapping[key] for key in relevant_keys}
        
        # Create class_name subdirectories
        for class_name in classes:
            class_dir = animal_dir / class_name
            if not class_dir.exists():
                class_dir.mkdir(parents=True, exist_ok=True)
                #print(f"Created class folder: {class_dir}")
        
        # Flag to track if an 'other_object' folder is needed
        other_object_needed = False
        other_object_dir = None
        
        # Iterate through .JPG files in animal_dir
        for jpg_file in animal_dir.glob('*.JPG'):
            base_filename = create_base_filename(jpg_file.name)  # Use create_base_filename here
            key = (camera_site, base_filename)
            if key in mapping:
                class_name = mapping[key]
                destination_dir = animal_dir / class_name
                destination_path = destination_dir / jpg_file.name
                #print(f"Moving {jpg_file} to {destination_dir}")
                try:
                    shutil.move(str(jpg_file), str(destination_path))
                except Exception as e:
                    print(f"Error moving {jpg_file}: {e}")
            else:
                # Flag the need for 'other_object' and create it lazily
                if not other_object_needed:
                    other_object_dir = animal_dir / "other_object"
                    other_object_dir.mkdir(parents=True, exist_ok=True)
                    print(f"Created 'other_object' folder: {other_object_dir}")
                    other_object_needed = True
                # Move file to 'other_object' folder
                destination_path = other_object_dir / jpg_file.name
                #print(f"Moving {jpg_file} to 'other_object'")
                try:
                    shutil.move(str(jpg_file), str(destination_path))
                except Exception as e:
                    print(f"Error moving {jpg_file} to 'other_object': {e}")

def main():
    config = load_config()
    service_folders_base = config.get("service_directory")
    dataframe_filename = f"{config.get('output_table')}.csv"

    if not service_folders_base or not dataframe_filename:
        print("Configuration file is missing required fields: 'service_directory' and/or 'output_table'.")
        sys.exit(1)

    dataframe_path = Path(dataframe_filename)

    df = load_dataframe(dataframe_path) 
    mapping = prepare_mapping(df)
    process_animal_directories(service_folders_base, mapping)
    
    print("\nAll animal directories have been processed and classifications updated.")

if __name__ == "__main__":
    main()
