import piexif
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
from common import load_config, SanityCheckError

def load_site_table(site_table_path):
    """Load the site table CSV and verify required columns."""
    required_columns = {'camera_site', 'lat', 'lon'}  # Changed from site_name
    try:
        site_table = pd.read_csv(site_table_path)
    except Exception as e:
        print(f"Error loading site table: {e}")
        raise SanityCheckError()
    if not required_columns.issubset(site_table.columns):
        print(f"Site table must contain columns: {required_columns}")
        raise SanityCheckError()
    return site_table

def get_site_directories(service_directory):
    """Get all site directories that contain md_out.json by walking the full tree."""
    service_path = Path(service_directory)
    site_dirs = {}
    
    # Walk through all subdirectories
    for path in service_path.rglob('md_out.json'):
        # Parent directory of md_out.json is the site directory
        site_dir = path.parent
        site_name = site_dir.name
        site_dirs[site_name] = site_dir
    
    return site_dirs

def perform_sanity_checks(site_table, site_dirs):
    """Perform sanity checks on site names and directories."""
    table_sites = set(site_table['camera_site'])  # Changed from site_name
    dir_sites = set(site_dirs.keys())

    # Check for missing site directories
    missing_dirs = table_sites - dir_sites
    if missing_dirs:
        print(f"Missing site directories for sites: {missing_dirs}")
        raise SanityCheckError()

    # Check for missing site names in site table
    missing_sites = dir_sites - table_sites
    if missing_sites:
        print(f"Site directories not listed in site table: {missing_sites}")
        raise SanityCheckError()

    # Check for required subdirectories
    for site_name, site_dir in site_dirs.items():
        subdirs = [d.name for d in site_dir.iterdir() if d.is_dir()]
        if not any(sub in subdirs for sub in ['animal', 'blank']):
            print(f"Site '{site_name}' does not contain 'animal' or 'blank' subdirectories.")
            raise SanityCheckError()

def get_image_files(site_dir):
    """Recursively get all image files under a site directory."""
    image_extensions = {'.jpg', '.jpeg', '.png'}
    return [f for f in site_dir.rglob('*') if f.is_file() and f.suffix.lower() in image_extensions]

def extract_image_info(image_files):
    """Extract timestamps and categorize images."""
    first_timestamp = None
    last_timestamp = None
    timestamps = []
    animal_dates = set()
    event_dates = set()
    counts = defaultdict(int)

    for img_file in image_files:
        # Check if file is in any subfolder under 'animal'
        is_animal = 'animal' in img_file.parent.parts
        # For other categories, check immediate parent folder
        category = img_file.parent.name.lower()
        
        # Categorize the image
        if is_animal:
            category = 'animal'
        elif category not in ['blank', 'person', 'vehicle']:
            continue  # Skip irrelevant folders
            
        counts[category] += 1

        # Extract timestamp using piexif
        try:
            exif_data = piexif.load(str(img_file))
            date_time_orig = exif_data.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal, None)
            if date_time_orig:
                timestamp = datetime.strptime(date_time_orig.decode('utf-8'), "%Y:%m:%d %H:%M:%S")
            else:
                timestamp = None
        except Exception:
            timestamp = None

        if timestamp:
            timestamps.append(timestamp)
            if not first_timestamp or timestamp < first_timestamp:
                first_timestamp = timestamp
            if not last_timestamp or timestamp > last_timestamp:
                last_timestamp = timestamp

            # Track dates for animal and event images
            date_str = timestamp.strftime('%Y-%m-%d')
            if category == 'animal':
                animal_dates.add(date_str)
            event_dates.add(date_str)

    return {
        'first_image': first_timestamp.strftime('%d/%m/%Y %H:%M:%S') if first_timestamp else None,
        'last_image': last_timestamp.strftime('%d/%m/%Y %H:%M:%S') if last_timestamp else None,
        'op_days': (last_timestamp - first_timestamp).days if first_timestamp and last_timestamp else None,
        'animal': counts['animal'],
        'days_with_animal': len(animal_dates),
        'blank': counts['blank'],
        'person': counts['person'],
        'vehicle': counts['vehicle'],
        'total_images': sum(counts.values()),
        'days_with_event': len(event_dates),
    }

def update_site_table(site_table, site_dirs):
    """Update the site table with new columns."""
    new_columns = ['first_image', 'last_image', 'op_days', 'animal', 'days_with_animal',
                   'blank', 'person', 'vehicle', 'total_images', 'days_with_event']
    for col in new_columns:
        site_table[col] = None  # Initialize new columns

    for index, row in tqdm(site_table.iterrows(), total=site_table.shape[0], desc="Processing sites"):
        site_name = row['camera_site']
        site_dir = site_dirs.get(site_name)
        if not site_dir:
            print(f"Skipping site '{site_name}' as it is missing in service directory.")
            continue
        image_files = get_image_files(site_dir)
        image_info = extract_image_info(image_files)
        for col in new_columns:
            site_table.at[index, col] = image_info[col]
    return site_table

def main():
    config = load_config()
    service_directory = config.get('service_directory')
    site_table_path = config.get('site_table')

    if not service_directory or not site_table_path:
        print("Error: 'service_directory' and/or 'site_table' is missing in configuration.")
        raise SanityCheckError()

    print("Gathering site information and checking baseline site-table data...")
    # Load site table
    site_table = load_site_table(site_table_path)

    # Get site directories
    site_dirs = get_site_directories(service_directory)

    # Perform sanity checks
    perform_sanity_checks(site_table, site_dirs)

    # Update site table with new columns
    updated_site_table = update_site_table(site_table, site_dirs)

    # Save updated site table
    updated_site_table.to_csv(site_table_path, index=False)
    print("Site table has been updated and saved.")

if __name__ == "__main__":
    main()
