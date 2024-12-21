import shutil, pickle
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from common import load_config, SanityCheckError

class SanityCheckError(Exception):
    """Custom exception for sanity check failures."""
    pass

def sanity_check_species_breakout(classified_snips_path):
    """
    Ensure that each species folder is flat and contains at least one snip.
    Abort if any species folder contains subfolders with snips.
    Delete empty subfolders or empty species folders if any.
    """
    print("Checking format and completion of expert-checked species breakout directory...")
    species_folders = [f for f in Path(classified_snips_path).iterdir() if f.is_dir()]

    # Check for subfolders and delete if empty
    for species in tqdm(species_folders, desc="Checking species folders"):
        subfolders = [sub for sub in species.iterdir() if sub.is_dir()]
        for sub in subfolders:
            snip_files = list(sub.glob("*.*"))  # Assuming snips have file extensions
            if snip_files:
                raise SanityCheckError(
                    "Aborted: One or more subfolders of the species breakout have not been sorted into their species folders. "
                    "Please complete the task before continuing."
                )
            else:
                shutil.rmtree(sub)  # Delete empty subfolder

        # Check if the species folder itself is empty and delete if needed
        if not any(species.iterdir()):  # Check if the folder is now empty
            shutil.rmtree(species)

    print("Check passed. Species breakout directory is properly organised.\n")

def create_randname_classname_table(classified_snips_path):
    """
    Create a DataFrame mapping rand_name (filename) to class_name (species).
    """
    print("Creating rand_name : class_name keypair table...")
    data = []
    species_folders = [f for f in Path(classified_snips_path).iterdir() if f.is_dir()]
    for species in tqdm(species_folders, desc="Processing species folders"):
        snip_files = list(species.glob("*.*"))  # Assuming snips have file extensions
        for snip in snip_files:
            data.append({
                'rand_name': snip.name,
                'class_name': species.name
            })
    df = pd.DataFrame(data)
    print(f"Created keypair table with {len(df)} entries.\n")
    return df

def create_consolidated_species_table(service_directory):
    """
    Combine all mewc_out.csv files into a single DataFrame with additional columns.
    After consolidation, remove unneeded columns and ensure proper naming conventions.
    
    Parameters:
    - service_directory (str or Path): Path to the directory containing all camera sites.
    
    Returns:
    - pd.DataFrame: Consolidated DataFrame ready for further processing.
    """
    print("Creating consolidated species table from all cameras...")
    consolidated_data = []
    
    # Find all mewc_out.csv files within the service_directory tree
    mewc_files = list(Path(service_directory).rglob("mewc_out.csv"))
    print(f"Found {len(mewc_files)} 'mewc_out.csv' files.\n")
    
    for mewc_file in tqdm(mewc_files, desc="Processing mewc_out.csv files"):
        # Determine the camera_site based on the directory structure
        # Adjust this as per your actual directory hierarchy
        # Example: service_directory/CameraSite/mewc_out.csv --> camera_site = 'CameraSite'
        camera_site = mewc_file.parent.name
        
        # Read the CSV file
        try:
            df = pd.read_csv(mewc_file)
        except Exception as e:
            print(f"Error reading '{mewc_file}': {e}. Skipping this file.")
            continue
        
        # Assign the camera_site to each row
        df['camera_site'] = camera_site
        
        # Append to the consolidated data list
        consolidated_data.append(df)
    
    if not consolidated_data:
        print("No data to consolidate. Exiting.")
        raise SanityCheckError()
    
    # Concatenate all DataFrames
    consolidated_df = pd.concat(consolidated_data, ignore_index=True)
      
    # Handle the unnamed index column if it exists
    if 'Unnamed: 0' in consolidated_df.columns:
        # Assuming 'Unnamed: 0' is the original index, drop it
        consolidated_df.drop(columns=['Unnamed: 0'], inplace=True)
    
    # Ensure 'camera_site' is the first column
    columns = list(consolidated_df.columns)
    if 'camera_site' in columns:
        columns.insert(0, columns.pop(columns.index('camera_site')))
        consolidated_df = consolidated_df[columns]
    else:
        print("Warning: 'camera_site' column not found in the data.")
    
    # Delete unneeded columns: 'label' and 'class_rank' if they exist
    columns_to_delete = ['label', 'class_rank']
    existing_columns_to_delete = [col for col in columns_to_delete if col in consolidated_df.columns]
    if existing_columns_to_delete:
        consolidated_df.drop(columns=existing_columns_to_delete, inplace=True)
    else:
        print("No unneeded columns ('label', 'class_rank') found to drop.")
    
    # Add 'expert_updated' and 'event' columns with default values
    consolidated_df['expert_updated'] = -1
    consolidated_df['event'] = 1
    
    print(f"Consolidated table created with {len(consolidated_df)} entries.\n")
    return consolidated_df

def compare_and_update_classifications(consolidated_df, keypair_df):
    """
    Update class_name and expert_updated based on human corrections.
    """
    print("Comparing AI classifications with human corrections...")
    
    # Merge consolidated_df with keypair_df on 'rand_name'
    merged_df = consolidated_df.merge(keypair_df, on='rand_name', how='left', suffixes=('_ai', '_human'))
    
    # Identify changes where class_name_human is not NaN and differs from class_name_ai
    condition_changed = (merged_df['class_name_human'].notna()) & (merged_df['class_name_ai'] != merged_df['class_name_human'])
    merged_df.loc[condition_changed, 'class_name_ai'] = merged_df.loc[condition_changed, 'class_name_human']
    merged_df.loc[condition_changed, 'expert_updated'] = 1
    
    # Identify agreements where class_name_human is not NaN and matches class_name_ai
    condition_agreed = (merged_df['class_name_human'].notna()) & (merged_df['class_name_ai'] == merged_df['class_name_human']) & (merged_df['expert_updated'] == -1)
    merged_df.loc[condition_agreed, 'expert_updated'] = 0
    
    # Count changes
    num_changes = condition_changed.sum()
    print(f"Number of classifications updated by human: {num_changes}\n")
    
    # Drop 'class_name_human' column and rename 'class_name_ai' to 'class_name'
    merged_df.drop(columns=['class_name_human'], inplace=True)
    merged_df.rename(columns={'class_name_ai': 'class_name'}, inplace=True)
    
    # Remove rows where 'expert_updated' is -1 (inanimate/false detections)
    before_drop = len(merged_df)
    merged_df = merged_df[merged_df['expert_updated'] != -1].reset_index(drop=True)
    after_drop = len(merged_df)
    print(f"Removed {before_drop - after_drop} inanimate/false detection snips from the table.\n")
    
    return merged_df

def determine_independent_events(consolidated_df, interval_minutes=5, prob_threshold=0.2):
    """
    Assign event numbers based on time intervals and classification changes using vectorized operations.

    Parameters:
    - consolidated_df (pd.DataFrame): The consolidated classification table.
    - interval_minutes (int): Time interval in minutes to separate events.
    - prob_threshold (float): Probability threshold for low-confidence classifications.

    Returns:
    - pd.DataFrame: Updated DataFrame with assigned event numbers.
    """
    print("Determining independent events using timestamp and class_name sequence...")

    # Check for required columns
    required_columns = ['date_time_orig', 'camera_site', 'class_name', 'prob', 'expert_updated']
    missing_columns = [col for col in required_columns if col not in consolidated_df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {', '.join(missing_columns)}")
        raise SanityCheckError()

    # Parse the 'date_time_orig' column into datetime format
    consolidated_df['timestamp'] = pd.to_datetime(
        consolidated_df['date_time_orig'], format="%Y:%m:%d %H:%M:%S", errors='coerce'
    )

    # Check for any parsing failures
    if consolidated_df['timestamp'].isna().any():
        num_na = consolidated_df['timestamp'].isna().sum()
        print(f"Warning: {num_na} timestamps could not be parsed and will be excluded.")
        consolidated_df = consolidated_df.dropna(subset=['timestamp'])

    # Sort the DataFrame by 'camera_site' and 'timestamp' to process chronologically
    consolidated_df.sort_values(['camera_site', 'timestamp'], inplace=True)
    consolidated_df.reset_index(drop=True, inplace=True)

    # Calculate time differences within each camera_site
    consolidated_df['time_diff'] = consolidated_df.groupby('camera_site')['timestamp'].diff().fillna(pd.Timedelta(minutes=0))

    # Calculate previous class_name within each camera_site
    consolidated_df['prev_class_name'] = consolidated_df.groupby('camera_site')['class_name'].transform('shift')

    # Determine class changes, excluding transitions involving 'unknown_animal'
    consolidated_df['class_change'] = (
        (consolidated_df['class_name'] != consolidated_df['prev_class_name']) &
        (consolidated_df['class_name'] != 'unknown_animal') &
        (consolidated_df['prev_class_name'] != 'unknown_animal')
    )

    # Flag expert updates that should trigger event changes
    consolidated_df['expert_update_flag'] = (
        (consolidated_df['expert_updated'] == 1) |
        ((consolidated_df['expert_updated'] == 0) & (consolidated_df['prob'] > prob_threshold))
    )

    # Determine where new events should start
    consolidated_df['new_event'] = (
        (consolidated_df['time_diff'] > pd.Timedelta(minutes=interval_minutes)) |
        (consolidated_df['class_change'] & consolidated_df['expert_update_flag'])
    )

    # Identify the first snip in each camera_site
    consolidated_df['is_first_snip'] = consolidated_df.groupby('camera_site').cumcount() == 0

    # Ensure that 'new_event' is False for the first snip in each camera_site
    consolidated_df['new_event'] = consolidated_df['new_event'] & (~consolidated_df['is_first_snip'])

    # Initialize the 'event' column to 1
    consolidated_df['event'] = 1

    # Iterate through each camera_site group to assign event numbers
    for camera_site, group in consolidated_df.groupby('camera_site'):
        # Initialize event counter
        current_event = 1

        # Iterate through the rows in the group
        for idx, row in group.iterrows():
            if row['new_event']:
                current_event += 1  # Increment event if new_event is True
            consolidated_df.at[idx, 'event'] = current_event

    # Clean up intermediate columns
    consolidated_df.drop(
        columns=['date_time_orig','time_diff', 'prev_class_name', 'class_change', 'expert_update_flag', 'new_event', 'is_first_snip'],
        inplace=True
    )

    print("Event assignment completed.\n")
    return consolidated_df

def refine_unknown_animal_classifications(consolidated_df, prob_threshold=0.2):
    """
    Within each event, refine 'unknown_animal' classifications based on context.
    """
    print("Refining 'unknown_animal' classifications within events...")
    
    grouped = consolidated_df.groupby(['camera_site', 'event'])
    for (camera, event), group in tqdm(grouped, desc="Processing events"):    
        # Identify dominant class excluding 'unknown_animal'
        dominant_classes = group[(group['class_name'] != 'unknown_animal') & (group['prob'] >= prob_threshold)]
       
        # Identify 'unknown_animal' entries
        unknown_animals = group[group['class_name'] == 'unknown_animal']
        indices_to_replace = unknown_animals.index

        if not dominant_classes.empty:
            # Use the most frequent dominant class
            replacement_class = dominant_classes['class_name'].mode()[0]          
            consolidated_df.loc[indices_to_replace, 'class_name'] = replacement_class
            # Update the probability for the replaced rows
            highest_prob_value = group.loc[group['prob'].idxmax(), 'prob']
            consolidated_df.loc[indices_to_replace, 'prob'] = highest_prob_value
            # Update the expert_updated flag to 2, to indicate an automated change to unknown_animal
            consolidated_df.loc[indices_to_replace, 'expert_updated'] = 2

    print("Refinement of 'unknown_animal' classifications completed.\n")
    return consolidated_df

def save_final_table(consolidated_df, table_path):
    """
    Save the consolidated table as CSV and pickle.
    """
    print("Saving the final consolidated, expert-verified ID site-speces table...")
    output_csv = Path(table_path).with_suffix(".csv")
    output_pickle = Path(table_path).with_suffix(".pkl")
    
    # Save CSV
    consolidated_df.to_csv(output_csv, index=False)
    print(f"Saved consolidated table to {output_csv}")
    
    # Save pickle
    with open(output_pickle, 'wb') as f:
        pickle.dump(consolidated_df, f)
    print(f"Saved consolidated table pickle to {output_pickle}")

def create_base_filename(filename):
    """
    Create base filename by removing suffix like -0, -1, etc.
    """
    parts = filename.split('.')
    if len(parts) < 2:
        return filename
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
    df['base_filename'] = df['filename'].apply(create_base_filename)
    df_sorted = df.sort_values(['camera_site', 'base_filename', 'prob'], ascending=[True, True, False])
    mapping_df = df_sorted.drop_duplicates(subset=['camera_site', 'base_filename'], keep='first')
    
    mapping = {}
    for _, row in mapping_df.iterrows():
        key = (row['camera_site'], row['base_filename'])
        mapping[key] = row['class_name']
    return mapping

def process_animal_directories(service_base_dir, mapping):
    """
    Process all \animal directories and organize files by class_name.
    """
    print("\nBreaking out animal folders into species subfolders...")
    service_base = Path(service_base_dir)
    animal_dirs = [animal_dir for animal_dir in service_base.rglob('animal') if animal_dir.is_dir()]
    
    if not animal_dirs:
        print("No 'animal' directories found. Ensure the directory structure is correct.")
        return
    
    for animal_dir in tqdm(animal_dirs, desc="Processing animal directories"):
        camera_site = animal_dir.parent.name
        print(f"\nProcessing 'animal' directory for camera_site: {camera_site}")
        
        relevant_keys = [key for key in mapping if key[0] == camera_site]
        classes = {mapping[key] for key in relevant_keys}
        
        for class_name in classes:
            class_dir = animal_dir / class_name
            if not class_dir.exists():
                class_dir.mkdir(parents=True, exist_ok=True)
        
        other_object_needed = False
        other_object_dir = None
        
        for jpg_file in animal_dir.glob("*.jp*g"):
            base_filename = create_base_filename(jpg_file.name)
            key = (camera_site, base_filename)
            if key in mapping:
                class_name = mapping[key]
                destination_dir = animal_dir / class_name
                destination_path = destination_dir / jpg_file.name
                try:
                    shutil.move(str(jpg_file), str(destination_path))
                except Exception as e:
                    print(f"Error moving {jpg_file}: {e}")
            else:
                if not other_object_needed:
                    other_object_dir = animal_dir / "other_object"
                    other_object_dir.mkdir(parents=True, exist_ok=True)
                    print(f"Created 'other_object' folder: {other_object_dir}")
                    other_object_needed = True
                destination_path = other_object_dir / jpg_file.name
                try:
                    shutil.move(str(jpg_file), str(destination_path))
                except Exception as e:
                    print(f"Error moving {jpg_file} to 'other_object': {e}")

def main():
    """Execute the complete workflow for creating species table and organizing folders."""
    config = load_config()
    
    # Get configuration parameters
    service_directory = config.get("service_directory")
    classified_snips_path = config.get("classified_snips_path")
    output_table = config.get("output_table")
    
    if not all([service_directory, classified_snips_path, output_table]):
        print("Configuration file is missing required fields.")
        raise SanityCheckError()
    
    print("Phase 1: Creating species-site table...")
    # Step 1: Sanity Check
    sanity_check_species_breakout(classified_snips_path)

    # Step 2: Create Keypair Table
    keypair_df = create_randname_classname_table(classified_snips_path)

    # Step 3: Create Consolidated Table
    consolidated_df = create_consolidated_species_table(service_directory)
    
    # Step 4: Compare and Update Classifications
    consolidated_df = compare_and_update_classifications(consolidated_df, keypair_df)
    
    # Step 5: Determine Independent Events
    int_min = config.get("indep_event_interval_minutes", 5)
    p_thresh = config.get("low_confidence_prob_threshold", 0.2)
    consolidated_df = determine_independent_events(consolidated_df, interval_minutes=int_min, prob_threshold=p_thresh)

    # Step 6: Refine unknown_animal Classifications
    consolidated_df = refine_unknown_animal_classifications(consolidated_df, prob_threshold=p_thresh)
    
    # Step 7: Save Final Table
    save_final_table(consolidated_df, output_table)
    
    print("\nPhase 2: Breaking out animal folders for each camera site...")
    # Step 8: Prepare mapping and process animal directories
    mapping = prepare_mapping(consolidated_df)
    process_animal_directories(service_directory, mapping)
    
    print("\nAll processing completed successfully!")

if __name__ == "__main__":
    main()
