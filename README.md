# mewc-table
Post-process wildlife camera services after running MEWC.

# Camera Trap Post-Processing Workflow

This repository contains a set of Python scripts for post-processing camera trap data run through the MEWC detection and classification AI. It allows for workflows seek to include human verification after running the AI. The tools facilitate the reconciliation, refinement, and consolidation of classification tables while integrating human corrections and contextual updates.

## Project Purpose

The goal of this project is to streamline the processing of wildlife camera trap data, leveraging both automated AI detection and human expertise. It includes:

- Organising snip images into species breakout folders for human review.
- Updating consolidated classification tables based on species breakout corrections.
- Refining `unknown_animal` classifications using event-based context.
- Integrating EXIF metadata, such as timestamps and flash data.
- Producing a comprehensive species classification table for analysis.

## Workflow Overview

1. **Initial AI Classification**:
   - AI processes camera trap images into snip files and generates a preliminary classification table (`mewc_out.csv`) for each camera site.

2. **Snip Organization**:
   - Snip images are organized into folders based on species, with probability bins for human verification.

3. **Human Verification**:
   - Experts validate classifications, moving images into appropriate species folders or creating new folders for unidentified species.

4. **Reconciliation**:
   - Scripts reconcile the updated folder structure with the AI-generated classification tables, incorporating corrections.

5. **Refinement**:
   - Events are recalculated based on time intervals and contextual species information.
   - `unknown_animal` classifications are inferred based on event-level context.

6. **Final Table Production**:
   - A comprehensive table is produced with species classifications, timestamps, and metadata.

## Features

- **Robust Image and Metadata Handling**:
  - Supports EXIF metadata extraction (e.g., timestamps, flash data).
  - Handles variations in file naming conventions.

- **Event and Contextual Refinement**:
  - Calculates independent events based on time intervals.
  - Infers `unknown_animal` classifications using event-level species data.

- **Configurable and Scalable**:
  - Supports batch processing across multiple camera sites.
  - Fully configurable via `params.json`.

## Scripts

### 1. `1_copy_and_pool_snips.py`
This script consolidates snip files from the service directory, pools them into a central location, and prepares them for AI-based classification.

- Scans all camera site folders for `snips` directories.
- Copies snip files into a centralized directory with unique filenames.
- Ensures compatibility with the AI classification pipeline.

### 2. `2_snip_sort.py`
Organises snip files into species folders based on AI classification and probability bins.

- Reads the AI-generated classification table (`mewc_out.csv`).
- Sorts snip files into species folders.
- Organizes snips into probability bins for easy human verification.

### 3. `3_create_site-species_table.py`
Generates a consolidated species classification table from AI outputs.

- Merges all `mewc_out.csv` files across camera sites into a unified table.
- Adds metadata such as `camera_site`, `expert_updated`, and `event`.
- Prepares the initial classification table for human verification and refinement.

### 4. `4_breakout_animal_folders.py`
Processes the `\animal` folders for each camera site after human verification.

- Reorganises images into species-specific subfolders within the `\animal` folder.
- Moves unclassified or non-animal files to an `other_object` folder.
- Ensures alignment between folder structure and species classifications.

### 5. `5_update_output_table.py`
Reconciles the consolidated species table with updates from human-verified `\animal` folders.

- Updates species classifications based on the reorganization in `\animal`.
- Appends new rows for files moved into `\animal` from non-animal folders.
- Integrates EXIF metadata such as timestamps for newly added images.
- Recalculates events and refines `unknown_animal` classifications based on context.

### 6. `6_update_flash_fired.py`
Adds flash metadata to the consolidated classification table.

- Extracts flash data from EXIF metadata for each image in the `\animal` folders.
- Updates the consolidated species table with a `flash_fired` column (`1` for flash, `0` otherwise).

## Usage Instructions

### Prerequisites

- Python 3.9+
- Required libraries: `pandas`, `numpy`, `piexif`, `Pillow`, `tqdm`
- AI detection tool outputs: snips and `mewc_out.csv` tables.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-repo-name>.git
   cd <your-repo-name>
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Create a `params.json` file with the following structure:

```json
{
    "service_directory": "C:\\camera_traps\\service",
    "snip_pool": "C:\\camera_traps\\snip_pool\\snips",
    "csv_path": "C:\\camera_traps\\snip_pool\\mewc_out.csv",
    "classified_snips_path": "C:\\camera_traps\\species_breakout",
    "output_table": "C:\\camera_traps\\mewc_species-site_id_ausplots_feb24",
    "mewc_predict_powershell": {
        "script_path": "C:\\camera_traps\\mewc\\mewc_predict.ps1",
        "input_snips": "C:\\camera_traps\\snip_pool",
        "predict_env": "C:\\camera_traps\\mewc\\predict.env",
        "class_map": "C:\\camera_traps\\mewc\\class_map.yaml",
        "model_file": "C:\\camera_traps\\mewc\\vtt_mewc.keras",
        "gpu_id": "0"
    },
    "probability_bins": [99, 95, 90, 70, 50, 30, 10],
    "indep_event_interval_minutes": 5,
    "low_confidence_prob_threshold": 0.2
}
```

### Running the Scripts

#### Example call to run the first script
```bash
python 1_copy_and_pool_snips.py
```

## Outputs

- **Consolidated Species Table**:
  - File: `mewc_species-site_id.csv` (also saved as `.pkl`).
  - Includes columns:
    - `camera_site`: Identifier for each camera site.
    - `filename`: Original image filename.
    - `class_id`: Numeric identifier for the species.    - 
    - `class_name`: Final species classification.
    - `prob`: Probability of classification.
    - `timestamp`: Image capture datetime.
    - `flash_fired`: Whether the flash fired (`1` or `0`).
    - `expert_updated`: Correction flags.

## Debugging and Logging

- Verbose progress bars (`tqdm`) for tracking.
- Debugging print statements for file and classification reconciliation.

## Future Enhancements

- Improved error handling and logging.
- Integration with cloud-based storage solutions.

---

Feel free to contribute to this repository by submitting pull requests or issues. For questions, reach out to `<barry.brook@utas.edu.au>`.

