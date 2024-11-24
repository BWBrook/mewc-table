import json, shutil, sys, pickle
from pathlib import Path
import pandas as pd
from tqdm import tqdm

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
                print(f"Aborted: One or more subfolders of the species breakout have not been sorted into their species folders. Please complete the task before continuing.")
                sys.exit(1)
            else:
                # Delete empty subfolder
                shutil.rmtree(sub)
                print(f"Deleted empty subfolder: {sub}")
        
        # Check if the species folder itself is empty and delete if needed
        if not any(species.iterdir()):  # Check if the folder is now empty
            shutil.rmtree(species)
            #print(f"Deleted empty species folder: {species}")
    
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
        sys.exit(1)
    
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
        sys.exit(1)

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
    print(f"Saved consolidated table pickle to {output_pickle}\n")

def load_config():
    # [Assuming params.json is in the same directory as the script]
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

def main():
    # Load configuration
    config = load_config()
    service_directory = config.get("service_directory")
    classified_snips_path = config.get("classified_snips_path")
    output_table = config.get("output_table")
    
    if not service_directory or not classified_snips_path:
        print("Configuration file is missing required fields: 'service_directory' and/or 'classified_snips_path'.")
        sys.exit(1)
    
    # Step 1: Sanity Check
    sanity_check_species_breakout(classified_snips_path)

    # Step 2: Create Keypair Table
    keypair_df = create_randname_classname_table(classified_snips_path)

    # Step 3: Create Consolidated Table
    consolidated_df = create_consolidated_species_table(service_directory)
    
    # Step 4: Compare and Update Classifications
    consolidated_df = compare_and_update_classifications(consolidated_df, keypair_df)
    
    # Step 5: Determine Independent Events
    int_min = config.get("indep_event_interval_minutes")
    p_thresh = config.get("low_confidence_prob_threshold")

    consolidated_df = determine_independent_events(consolidated_df, interval_minutes=int_min, prob_threshold=p_thresh)

    # Step 6: Refine 'unknown_animal' Classifications
    consolidated_df = refine_unknown_animal_classifications(consolidated_df, prob_threshold=p_thresh)
    
    # Step 7: Save Final Table
    save_final_table(consolidated_df, output_table)
    
    print("All steps completed successfully!")

if __name__ == "__main__":
    main()
