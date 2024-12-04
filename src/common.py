import os, subprocess, sys, yaml
from dotenv import load_dotenv
from pathlib import Path

class SanityCheckError(Exception):
    """Custom exception for sanity check failures."""
    pass

# Load .env file if present
load_dotenv()

def load_config():
    """Load configuration parameters from params.yaml, with environment variable overrides."""
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / 'params.yaml'

    if not config_path.is_file():
        print(f"Configuration file '{config_path}' does not exist.")
        sys.exit(1)

    try:
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{config_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error reading '{config_path}': {e}")
        sys.exit(1)

    for key in params.keys():
        env_var = os.getenv(key.upper())
        if env_var is not None:
            if isinstance(params[key], list):
                params[key] = [int(x) for x in env_var.split(",")]
            elif isinstance(params[key], (int, float)):
                params[key] = type(params[key])(env_var)
            else:
                params[key] = env_var

    # Ensure PROBABILITY_BINS is null in auto mode
    if os.getenv("WORKFLOW_MODE", "manual").lower() == "auto":
        params["probability_bins"] = []

    return params

# Map script numbers to filenames and descriptions
SCRIPT_MAP = {
    "1": ("1_breakout_snips.py", "Breakout snips into AI-predicted species bins with probability classes, for expert checking and re-arrangement. (Must have already run the MEWC-service workflow on the folders.)\n"),
    "2": ("2_create_table_and_animal_subfolders.py", "Create a consolidated species-site table, taking into account expert updates to the snips, and breakout each of the camera-site animal folders into species sub-folders. (Option 1 must have already been run.)\n"),
    "3": ("3_update_output_table.py", "Update the species-site table to account for expert updates to each camera site's animal folder species subfolders, including false-blank recovery and species re-arrangements. (Option 2 must have already been run.)\n"),
    "4": ("4_make_site_table.py", "Optional: Create a site-statistics table, including operating data for each camera. (Must supply a base site table with a minimum of camera_site, lat and lon columns as a .CSV file.)\n")
}

def run_script(script_id):
    """Run a script based on its ID."""
    script_info = SCRIPT_MAP.get(script_id.strip())
    if script_info:
        script_file, description = script_info
        print(f"Running {script_file}...")
        subprocess.run(["python", f"/app/src/{script_file}"], check=True)

def is_interactive():
    """Check if the script is running in an interactive terminal."""
    return sys.stdin.isatty()

if __name__ == "__main__":
    workflow_mode = os.getenv("WORKFLOW_MODE", "manual").lower()
    scripts_to_run = os.getenv("RUN_SCRIPTS", "1").split(",")

    if workflow_mode == "manual":
        if is_interactive():
            print("Manual mode: Select scripts to run.\n")
            print("Available options:\n")
            for script_id, (script_file, description) in SCRIPT_MAP.items():
                print(f"{script_id} = {description}")
            
            # Let the user select scripts interactively
            selected_scripts = input("Enter the script number(s) to run (e.g., 1 or comma-separated, e.g., 1,2,3): ").split(",")
            for script in selected_scripts:
                run_script(script)
        else:
            print("Manual mode: Non-interactive environment detected. Exiting.\n")
    else:
        print(f"Automated mode: Running scripts {scripts_to_run} sequentially.\n")
        for script in scripts_to_run:
            run_script(script)
