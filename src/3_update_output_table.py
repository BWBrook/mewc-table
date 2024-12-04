import os, piexif
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from PIL import Image
from tqdm import tqdm
from common import load_config, SanityCheckError

# Utility functions
def load_dataframe(output_table_path):
    """Load the consolidated species table as a pandas DataFrame."""
    csv_path = Path(str(output_table_path) + ".csv")
    pkl_path = Path(str(output_table_path) + ".pkl")

    if csv_path.exists():
        return pd.read_csv(csv_path)
    elif pkl_path.exists():
        return pd.read_pickle(pkl_path)
    else:
        raise FileNotFoundError("No valid .csv or .pkl file found for output_table.")

def save_dataframe(df, output_table_path):
    """Save the updated DataFrame as both CSV and Pickle files."""
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
    
    # Handle alternative format without chained assignment
    if nat_mask.any():
        alternative_parsed = pd.to_datetime(
            df.loc[nat_mask, 'timestamp'],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
        df.loc[nat_mask, 'timestamp_parsed'] = alternative_parsed

    # Sort by datetime and format consistently
    df = df.sort_values(['camera_site', 'timestamp_parsed'])
    df['timestamp'] = df['timestamp_parsed'].dt.strftime('%d/%m/%Y %H:%M:%S')
    df = df.drop(columns=['timestamp_parsed'])

    return df

def reconcile_table(df, file_mapping):
    updated_rows = []
    df_columns = df.columns.tolist()
    updates_count = 0  # Counter for updates

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Reconciling table"):
        base_filename = create_base_filename(row['filename'])
        camera_site = row['camera_site']

        if (base_filename, camera_site) in file_mapping:
            file, mapped_camera_site, class_name = file_mapping[(base_filename, camera_site)]
            if mapped_camera_site == camera_site:
                if row['class_name'] == class_name:
                    updated_rows.append(row.to_dict())
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
                    updates_count += 1  # Increment counter
                file_mapping.pop((base_filename, camera_site))
            else:
                updated_rows.append(row.to_dict())
        else:
            updated_rows.append(row.to_dict())

    # Count new rows added
    new_rows_count = sum(1 for (_, _), (_, _, class_name) in file_mapping.items() 
                        if class_name != "other_object")

    print(f"\nReconciliation summary:")
    print(f"  - Updated classifications: {updates_count}")
    print(f"  - New rows added: {new_rows_count}")
    print(f"  - Total changes: {updates_count + new_rows_count}\n")

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
    inferred_count = 0  # Counter for inferred unknowns

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
        valid_species = group[(group['class_name'] != 'unknown_animal') & (group['prob'] >= thresh)]
        
        if not valid_species.empty:
            replacement_class = valid_species['class_name'].mode()[0]
            replacement_prob = valid_species['prob'].max()

            unknown_indices = group[group['class_name'] == 'unknown_animal'].index
            if len(unknown_indices) > 0:
                inferred_count += len(unknown_indices)
                reconciled_df.loc[unknown_indices, 'class_name'] = replacement_class
                reconciled_df.loc[unknown_indices, 'prob'] = replacement_prob
                reconciled_df.loc[unknown_indices, 'expert_updated'] = 5

    print(f"\nEvent processing summary:")
    print(f"  - Total events processed: {len(grouped)}")
    print(f"  - Unknown animals inferred: {inferred_count}\n")

    # Clean up intermediate columns
    reconciled_df.drop(columns=['time_diff', 'new_event'], inplace=True)

    # Format all timestamps as 'DD/MM/YYYY HH:MM:SS'
    reconciled_df['timestamp'] = reconciled_df['timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')

    print("Event recalculation and refinement completed.")
    return reconciled_df

def count_animals_per_event(df):
    """
    Deduplicate rows with identical timestamps within same event while tracking duplicate count.
    """
    GROUP_COLS = ['camera_site', 'class_name', 'event', 'timestamp']
    
    df = df.copy() # Make a copy to avoid modifying original
    
    # Drop count column if it already exists
    if 'count' in df.columns:
        df = df.drop(columns=['count'])
    
    # Insert count column after class_name
    df.insert(df.columns.get_loc('class_name') + 1, 'count', 1)
    
    # Process each camera site
    all_camera_sites = df['camera_site'].unique()
    
    for site in tqdm(all_camera_sites, desc="Processing camera sites to count animals on each image..."):
        # Get data for this site
        site_mask = df['camera_site'] == site
        
        # Group and count duplicates using GROUP_COLS
        counts = df[site_mask].groupby(GROUP_COLS).size()
        
        # Find first occurrence of each timestamp group
        duplicates = df[site_mask].duplicated(GROUP_COLS, keep='first')
        
        # Get indices of rows to update
        update_idx = df[site_mask & ~duplicates].index
        
        # Update counts directly using index alignment
        for idx in update_idx:
            key = tuple(df.loc[idx, GROUP_COLS])
            df.loc[idx, 'count'] = counts[key]
    
    # Remove duplicate rows
    result = df[~df.duplicated(GROUP_COLS)].copy()
    
    # Final sanity check - align indices before comparing
    original_counts = df.groupby(GROUP_COLS).size()
    result_counts = result.set_index(GROUP_COLS)['count']    
    # Reindex both Series to have the same index
    common_index = original_counts.index.intersection(result_counts.index)
    original_aligned = original_counts[common_index]
    result_aligned = result_counts[common_index]
    
    mismatches = (original_aligned != result_aligned).sum()
    
    if mismatches > 0:
        print(f"\nWARNING: Found {mismatches} count mismatches in final verification, aborting script.")
        print("Original counts vs Result counts:")
        mismatch_idx = original_aligned != result_aligned
        print(pd.DataFrame({
            'Original': original_aligned[mismatch_idx],
            'Result': result_aligned[mismatch_idx]
        }))
        raise SanityCheckError()
    
    print(f"\nTotal rows in final consolidated MEWC table: {len(result)}")
    print(f"Observations with count > 1: {len(result[result['count'] > 1])}")
    print(f"Max number of detections in a single image: {result['count'].max()}")
    
    return result

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

def update_flash_fired(service_directory, df):
    """
    Update the DataFrame with a 'flash_fired' column for all images in \animal folders.
    
    Parameters:
    - service_directory: Path to the service directory
    - df: DataFrame to update (already in memory)
    
    Returns:
    - Updated DataFrame with flash_fired column
    """
    print("Updating flash_fired data...")
    
    # Initialize flash_fired column
    df['flash_fired'] = -1  # Default to -1 for rows not matched to any image
    
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
            mask = (df['filename'].apply(create_base_filename) == base_filename) & \
                   (df['camera_site'] == camera_site)
            df.loc[mask, 'flash_fired'] = flash_fired_value

    print("Flash data updated for all matching rows.")
    return df

def main():
    """Execute the complete workflow for updating output table."""
    config = load_config()

    service_directory = config.get('service_directory')
    output_table_path = Path(config.get('output_table'))

    if not service_directory or not output_table_path:
        print("Configuration file is missing required fields: 'service_directory' and/or 'output_table'.")
        raise SanityCheckError()

    print("\nPhase 1: Updating output table...")
    # Load the consolidated table
    df = load_dataframe(output_table_path)

    # Scan the \animal folders
    file_mapping = scan_animal_folders(service_directory)

    # Reconcile the table
    reconciled_df = reconcile_table(df, file_mapping)

    # Recalculate events and refine classifications
    int_min = config.get('indep_event_interval_minutes')
    p_thresh = config.get('low_confidence_prob_threshold')
    reconciled_df = recalc_events_and_infer_unknowns(reconciled_df, int_min, p_thresh)

    # Count animals per event and remove duplicate rows
    reconciled_df = count_animals_per_event(reconciled_df)
    
    # Phase 2: Adding flash fired data
    print("\nPhase 2: Adding flash fired data to adjusted classifications...")
    reconciled_df = update_flash_fired(service_directory, reconciled_df)
    
    # Save the final updated table
    save_dataframe(reconciled_df, output_table_path)
    print("\nAll updates completed successfully!")

if __name__ == "__main__":
    main()
