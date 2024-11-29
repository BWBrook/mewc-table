import json, sys
from pathlib import Path

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