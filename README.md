<img src="mewc_logo_hex.png" alt="MEWC Hex Sticker" width="200" align="right"/>

# MEWC Table

## **Introduction**

A streamlined, Docker-driven workflow for post-processing AI-classified wildlife camera trap data from [**MEWC**](https://github.com/zaandahl/mewc). This repository provides tools to integrate automated AI detection with human expertise, facilitating the refinement and analysis of species classification and camera-trap metadata.

---

## **Project Overview**

`mewc-table` processes camera-trap data that has been run through the MEWC detection and classification AI. It allows users to:

- Organise snip images into species breakout folders for human review.
- Update consolidated classification tables based on human corrections.
- Recalculate events, recover blanks, and refine `unknown_animal` classifications based on contextual information.
- Generate comprehensive species and site statistics tables for further analysis.

The tools are designed to enhance workflow efficiency and ensure the accuracy of classification data.

---

## **Workflow**

The workflow consists of four scripts, each addressing a specific stage of the post-processing pipeline:

1. **Breakout Snips into Species Folders**
   - **Script**: `1_breakout_snips.py`
   - **Description**: Organises AI-classified snips into species folders with optional probability binning for expert verification.

2. **Create Species-Site Table and Animal Subfolders**
   - **Script**: `2_create_table_and_animal_subfolders.py`
   - **Description**: Generates a consolidated table from MEWC outputs and organizes camera site animal folders into species subfolders based on human corrections.

3. **Update Output Table**
   - **Script**: `3_update_output_table.py`
   - **Description**: Reconciles the consolidated table with updated animal folder classifications, blank recoveries, etc., and recalculates events, and infers `unknown_animal` identities.

4. **Generate Site Statistics Table** (Optional)
   - **Script**: `4_make_site_table.py`
   - **Description**: Produces a statistics table summarising camera site operations, including image counts, operating days, and event details (requires an initial CSV file with camera_name, lat, lon columns at a minimum).

---

## **Docker Setup and Usage**

The workflow is containerised for ease of deployment and consistent execution across systems.

### **Pull the Docker Image**

Pull the pre-built Docker image from DockerHub:
```bash
docker pull bwbrook/mewc-table:latest
```

### **Run the Workflow**

#### **Manual Mode**
Allows users to interactively select scripts to run:
```bash
docker run -it --rm --env-file /local_path/to/env/file -v /local_path/to/base_folder:/data bwbrook/mewc-table
```
At each step, after running 1 and then 2, the expert has the opportunity to intervene and adjust classification, recover undetected images from blank folders, etc.


#### **Automated Mode**
Executes a predefined sequence of scripts:
```bash
docker run --rm --env-file /local_path/to/env/file -v /local_path/to/data:/base_folder -e WORKFLOW_MODE=auto -e RUN_SCRIPTS="1,2,3,4" bwbrook/mewc-table
```

This option allows for a 'push-button' analysis without any human intervention, although this is not recommended.

---

## **Configuration**

### **params.yaml**
A baked-in configuration file provides default settings. Key parameters include:

- `service_directory`: Base directory for all camera data.
- `mewc_filename`: Name of the AI-generated classification file (default: `mewc_out.csv`).
- `classified_snips_path`: Directory for species breakout folders.
- `probability_bins`: Probability thresholds for binning classifications.
- `indep_event_interval_minutes`: Time separation between independent events.
- `output_table`: Path and filename for the consolidated species table.

### **Environment Overrides (.env)**
Users can override parameters in `params.yaml` by specifying them in an `.env` file. Example:

```env
SERVICE_DIRECTORY=/data/service
CLASSIFIED_SNIPS_PATH=/data/species_breakout
OUTPUT_TABLE=/data/output_table.csv
INDEP_EVENT_INTERVAL_MINUTES=10
```

Pass the `.env` file to the container using the `--env-file` flag. A full example .env is provided in the /env folder.

---

## **Outputs**

- **Consolidated Site-Species Table**:
  - File: `mewc_species-site_id.csv` (or your custom filename, also saved as `.pkl`).
  - Includes columns:
    - `camera_site`: Identifier for each camera site.
    - `filename`: Original image filename.
    - `class_id`: Numeric identifier for the species.
    - `class_name`: Final species classification.
    - `prob`: Probability of classification.
    - `count`: Number of detections on the image.
    - `flash_fired`: Whether the flash fired (`1` or `0`).
    - `expert_updated`: Correction flags, where:
      - 0 = no change from AI
      - 1 = expert updated the classification based on the snip
      - 2 = unknown animal was inferred based on event context, after snip sorting
      - 3 = expert updated the classification based on the full image, after \animal folder sorting
      - 4 = new image added to a species folder within the \animal folder from blanks, people or other_object
      - 5 = new image of unknown_animal updated to species based on event context.
    - `timestamp`: Date-time of the original camera-trap image.

- **Site Statistics Table** (Optional):
  - A summary table with operational data for each camera site.

- **Species Folders**:
  - Organised species breakout directories for expert validation.

---

## Debugging and Logging

- Verbose progress bars (`tqdm`) for tracking.
- Debugging print statements for file and classification reconciliation.

---

## **Future Enhancements**

Planned improvements include:
- Advanced error handling and logging.
- Enhanced visualisation tools for classification data.
- Vignette with example data folder for full processing demonstration.

---

Feel free to contribute to this repository by submitting pull requests or issues. For questions, contact `<barry.brook@utas.edu.au>`.
