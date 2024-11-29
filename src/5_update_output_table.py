import os, sys, piexif, json
import pandas as pd
from pathlib import Path
from PIL import Image
from datetime import datetime, timedelta
from tqdm import tqdm

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

def scan_animal_folders(service_directory):
    """
    Recursively scan all \animal folders and return a dictionary mapping
    base_filename -> (full_path, camera_site, class_name).
    """
    file_mapping = {}
    service_path = Path(service_directory)
    animal_dirs = service_path.rglob("animal")

    for animal_dir in tqdm(animal_dirs, desc="Scanning animal folders"):
        camera_site = animal_dir.parent.name  # Get parent folder as camera_site
        for class_folder in animal_dir.iterdir():
            if class_folder.is_dir():
                class_name = class_folder.name
                for file in class_folder.glob("*.JPG"):
                    base_filename = create_base_filename(file.name)
                    file_mapping[(base_filename, camera_site)] = (file, camera_site, class_name)

    return file_mapping

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

def extract_timestamp(filepath):
    """
    Extract date_time_orig from EXIF data or fallback to file modification time.
    Return formatted timestamp or 'NA' if both are unavailable.
    """
    try:
        with Image.open(filepath) as img:
            exif_data = piexif.load(img.info.get("exif", b""))
            date_time_orig = exif_data.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal, None)
            
            if date_time_orig:
                # Decode and format EXIF date_time_orig
                return pd.to_datetime(date_time_orig.decode('UTF-8'), format="%Y:%m:%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        # Log or print if needed for debugging
        print(f"EXIF extraction failed for {filepath}: {e}")

    try:
        # Fallback to file modification time
        modified_time = os.path.getmtime(filepath)
        return datetime.fromtimestamp(modified_time).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        # Log or print if needed for debugging
        print(f"File modification time fallback failed for {filepath}: {e}")

    # If both EXIF and modification time are unavailable
    return "NA"

def parse_timestamps(reconciled_df):
    """
    Parse mixed-format timestamps, ensure consistency, and reformat as 'DD/MM/YYYY HH:MM'.
    """
    # Make a copy to avoid SettingWithCopyWarning
    df = reconciled_df.copy()
    
    # First attempt: Parse as 'DD/MM/YYYY HH:MM'
    df['timestamp_parsed'] = pd.to_datetime(
        df['timestamp'], 
        format='%d/%m/%Y %H:%M', 
        errors='coerce'
    )

    # Find rows that need second parsing attempt
    nat_mask = df['timestamp_parsed'].isna()
    
    # Handle alternative format using recommended syntax
    if nat_mask.any():
        alternative_parsed = pd.to_datetime(
            df['timestamp'][nat_mask],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
        df['timestamp_parsed'].mask(nat_mask, alternative_parsed, inplace=True)

    # Sort by datetime
    df.sort_values(['camera_site', 'timestamp_parsed'], inplace=True)

    # Format timestamps consistently
    df['timestamp'] = df['timestamp_parsed'].dt.strftime('%d/%m/%Y %H:%M:%S')
    
    # Drop working column
    df.drop(columns=['timestamp_parsed'], inplace=True)

    return df

def reconcile_table(df, file_mapping):
    updated_rows = []
    df_columns = df.columns.tolist()  # Extract column names for consistency

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Reconciling table"):
        base_filename = create_base_filename(row['filename'])
        camera_site = row['camera_site']  # Get the camera_site from the current row

        # Use (base_filename, camera_site) as the key
        if (base_filename, camera_site) in file_mapping:
            file, mapped_camera_site, class_name = file_mapping[(base_filename, camera_site)]
            if mapped_camera_site == camera_site:
                if row['class_name'] == class_name:
                    updated_rows.append(row.to_dict())  # Case 1: Leave row intact
                else:
                    # Case 2: Update row
                    row['class_name'] = class_name
                    row['class_id'] = (
                        0 if class_name == "unknown_animal" else (
                            df[df['class_name'] == class_name]['class_id'].iloc[0]
                            if class_name in df['class_name'].values
                            else -1
                        )
                    )
                    row['expert_updated'] = 3
                    updated_rows.append(row.to_dict())
                # Remove the file from the mapping since it's reconciled
                file_mapping.pop((base_filename, camera_site))
            else:
                # Camera site mismatch; leave the row intact
                updated_rows.append(row.to_dict())
        else:
            # If base_filename is not in file_mapping, leave the row intact
            updated_rows.append(row.to_dict())

    # Case 3: Append new rows for unmapped files, excluding 'other_object'
    for (base_filename, mapped_camera_site), (file, _, class_name) in file_mapping.items():
        if class_name == "other_object":
            continue  # Ignore files in 'other_object'

        timestamp = extract_timestamp(file)

        new_row = {
            'camera_site': mapped_camera_site,
            'filename': file.name,
            'class_id': (
                0 if class_name == "unknown_animal" else (
                    df[df['class_name'] == class_name]['class_id'].iloc[0]
                    if class_name in df['class_name'].values
                    else -1
                )    
            ),
            'prob': 1,
            'class_name': class_name,
            'rand_name': "none",
            'conf': 0,
            'expert_updated': 4,
            'event': 0,
            'timestamp': timestamp
        }
        updated_rows.append(new_row)

    # Ensure all rows have consistent keys
    for row in updated_rows:
        for col in df_columns:
            if col not in row:
                row[col] = "NA"  # Fill missing columns with NA
        for extra_col in set(row.keys()) - set(df_columns):
            del row[extra_col]  # Remove unexpected keys

    # Convert updated_rows back to a DataFrame
    reconciled_df = pd.DataFrame(updated_rows)
    reconciled_df = parse_timestamps(reconciled_df)
    reconciled_df.reset_index(drop=True, inplace=True)

    return reconciled_df

def recalc_events_and_infer_unknowns(reconciled_df, int_m=5, thresh=0.2):
    """
    Recalculate events and infer unknown_animal identities based on event context.
    
    Parameters:
    - reconciled_df (pd.DataFrame): The reconciled table.
    - interval_minutes (int): Time interval (in minutes) to define separate events.
    - prob_threshold (float): Minimum probability for a valid context species.

    Returns:
    - pd.DataFrame: Updated DataFrame with recalculated events and inferred unknowns.
    """
    print("Recalculating events and refining unknown_animal classifications...")

    # Ensure 'timestamp' is datetime with dayfirst=True to avoid warnings
    reconciled_df['timestamp'] = pd.to_datetime(reconciled_df['timestamp'], dayfirst=True, errors='coerce')
    
    # Sort by camera_site and timestamp
    reconciled_df.sort_values(['camera_site', 'timestamp'], inplace=True)
    reconciled_df.reset_index(drop=True, inplace=True)

    # Recalculate events
    reconciled_df['time_diff'] = reconciled_df.groupby('camera_site')['timestamp'].diff().fillna(pd.Timedelta(seconds=0))
    reconciled_df['new_event'] = reconciled_df['time_diff'] > timedelta(minutes=int_m)
    reconciled_df['event'] = reconciled_df.groupby('camera_site')['new_event'].cumsum() + 1

    # Refine unknown_animal classifications within events
    grouped = reconciled_df.groupby(['camera_site', 'event'])
    for (camera_site, event), group in tqdm(grouped, desc="Processing events"):
        # Identify valid species within the event
        valid_species = group[(group['class_name'] != 'unknown_animal') & (group['prob'] >= thresh)]
        
        # Determine most frequent species
        if not valid_species.empty:
            replacement_class = valid_species['class_name'].mode()[0]  # Most frequent class
            replacement_prob = valid_species['prob'].max()  # Highest probability

            # Update unknown_animal entries
            unknown_indices = group[group['class_name'] == 'unknown_animal'].index
            reconciled_df.loc[unknown_indices, 'class_name'] = replacement_class
            reconciled_df.loc[unknown_indices, 'prob'] = replacement_prob
            reconciled_df.loc[unknown_indices, 'expert_updated'] = 5  # New flag for inferred unknowns

    # Clean up intermediate columns
    reconciled_df.drop(columns=['time_diff', 'new_event'], inplace=True)

    # Format all timestamps as 'DD/MM/YYYY HH:MM:SS'
    reconciled_df['timestamp'] = reconciled_df['timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')

    print("Event recalculation and refinement completed.")
    return reconciled_df

def count_animals_per_event(df):
    """
    Deduplicate rows with identical timestamps within same event while tracking duplicate count.
    Has the effect of counting animals per event.
    
    Args:
        df: DataFrame with camera_site, event and timestamp columns
        
    Returns:
        DataFrame with duplicates removed and count column showing number of original instances
    """
    # Insert count column after class_name
    df.insert(df.columns.get_loc('class_name') + 1, 'count', 1)
    
    # Group by relevant columns and get size of each group
    grouped = df.groupby(['camera_site', 'event', 'timestamp']).size().reset_index(name='dup_count')
    
    # Create mask for rows to keep (first occurrence of each timestamp in event)
    keep_mask = ~df.duplicated(['camera_site', 'event', 'timestamp'])
    
    # Update count column with duplicate counts
    df.loc[keep_mask, 'count'] = grouped['dup_count']
    
    # Return deduplicated dataframe 
    return df[keep_mask].copy()

def main():
    # Load configuration
    config_path = Path(__file__).parent / 'params.json'
    with open(config_path, 'r') as f:
        params = json.load(f)

    service_directory = params['service_directory']
    output_table_path = Path(params['output_table'])

    # Load the consolidated table
    df = load_dataframe(output_table_path)

    # Scan the \animal folders
    file_mapping = scan_animal_folders(service_directory)

    # Reconcile the table
    reconciled_df = reconcile_table(df, file_mapping)

    # Recalculate events and refine classifications
    int_min = params['indep_event_interval_minutes']
    p_thresh = params['low_confidence_prob_threshold']
    reconciled_df = recalc_events_and_infer_unknowns(reconciled_df, int_min, p_thresh)

    # Count animals per event and remove duplicate rows
    reconciled_df = count_animals_per_event(reconciled_df)

    # Save the updated table
    save_dataframe(reconciled_df, output_table_path)

if __name__ == "__main__":
    main()
