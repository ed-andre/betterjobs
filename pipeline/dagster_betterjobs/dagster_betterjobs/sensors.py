import os
from dagster import sensor, RunRequest, RunConfig, SkipReason, SensorResult, DefaultSensorStatus, AssetSelection
from typing import List, Set
import glob
import time

@sensor(
    target=AssetSelection.assets("adhoc_company_urls"),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60
)
def adhoc_company_urls_sensor(context):
    """
    Monitors the input folder for new CSV files containing company information.
    Triggers the adhoc_company_urls asset when CSV files are detected.
    """
    # Get input folder path from environment variable or use relative path as fallback
    input_folder = os.getenv("ADHOC_INPUT_FOLDER")

    if not input_folder:
        # Use relative path as fallback
        # Assuming the code is run from the project root
        input_folder = os.path.join("pipeline", "dagster_betterjobs", "input")
        context.log.info(f"ADHOC_INPUT_FOLDER environment variable not set, using relative path: {input_folder}")

    if not os.path.exists(input_folder):
        context.log.error(f"Input folder not found at: {input_folder}")
        return SkipReason(f"Input folder not found at: {input_folder}")

    # Look for CSV files in the input folder
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    context.log.info(f"Found {len(csv_files)} CSV files in {input_folder}")

    if not csv_files:
        return SkipReason(f"No CSV files found in input folder.")

    # Get the list of file names for logging
    file_names = [os.path.basename(f) for f in csv_files]
    context.log.info(f"CSV files found: {', '.join(file_names)}")

    # Get file modification times
    file_mtimes = {}
    for file_path in csv_files:
        try:
            file_mtime = os.path.getmtime(file_path)
            file_mtimes[os.path.basename(file_path)] = file_mtime
        except Exception as e:
            context.log.error(f"Error getting modification time for {file_path}: {str(e)}")

    # Check if we have already processed these files
    cursor_value = context.cursor or ""

    # Parse the cursor - format is "filename:timestamp,filename:timestamp"
    processed_files = {}
    if cursor_value:
        try:
            for entry in cursor_value.split(","):
                if ":" in entry:
                    fname, mtime = entry.split(":", 1)
                    processed_files[fname] = float(mtime)
        except Exception as e:
            context.log.error(f"Error parsing cursor: {str(e)}, cursor value: {cursor_value}")
            processed_files = {}

    # Check for new or modified files
    new_files = []
    for fname, mtime in file_mtimes.items():
        # Consider a file new if it's not in processed files or has a newer mtime
        if fname not in processed_files or mtime > processed_files.get(fname, 0):
            new_files.append(fname)
            context.log.info(f"New or modified file detected: {fname}")

    if not new_files:
        return SkipReason("No new or modified CSV files to process.")

    # Update the cursor with current files and their mtimes
    new_cursor_entries = []
    for fname, mtime in file_mtimes.items():
        new_cursor_entries.append(f"{fname}:{mtime}")
    new_cursor = ",".join(new_cursor_entries)

    context.log.info(f"Processing {len(new_files)} CSV files: {', '.join(new_files)}")

    # Create a run request for the adhoc_company_urls asset
    run_request = RunRequest(
        run_key=f"adhoc_company_urls_{','.join(sorted(new_files))}_{int(time.time())}",
        tags={"source": "adhoc_company_urls_sensor", "files": ",".join(new_files)}
    )

    return SensorResult(
        run_requests=[run_request],
        cursor=new_cursor
    )
