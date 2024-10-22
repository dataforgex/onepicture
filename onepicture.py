#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
#===============================================================================
# Dan Zhao
# 10-22-2024
#===============================================================================
# This program scans a directory for image files, creates metadata, identifies duplicate files, 
# and moves redundant files to a specified directory.
# Duplicate files are identified based on their MD5 hash values (using the first and last 1 MB for faster processing) to ensure content-based matching.
# This code was improved and refactored with the assistance of OpenAI's language model, ChatGPT-4, to enhance readability and efficiency.
from typing import Optional
import os
import datetime as dt
import pandas as pd
import hashlib
from pathlib import Path
import shutil
import time
import concurrent.futures
import multiprocessing

# Directory paths 
PICTURE_DIRECTORY = r'/Volumes/DanExtDisk2/photo_Timeline/2024-08'
REDUNDANT_DIRECTORY = r'/Volumes/DanExtDisk2/onepicture_deleteme'
TIMELINE_DIRECTORY = r'/Volumes/DanExtDisk2/Photo_Time_line'
BATCH_SIZE = 1000
VERBOSE = True  # Toggle for detailed output

CPU_CORES = multiprocessing.cpu_count()  # Get the number of CPU cores available

def calculate_file_hash(file_path: str) -> Optional[str]:
    """
    Calculate the MD5 hash of a file using the first and last 1 MB for faster processing.
    
    Args:
        file_path (str): The path to the file.
    
    Returns:
        Optional[str]: The MD5 hash of the file, or None if the file cannot be found.
    """
    md5_hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # Hash the first and last 1 MB of the file
            head = f.read(1024 * 1024)
            if head:
                md5_hash.update(head)
            try:
                f.seek(-1024 * 1024, os.SEEK_END)
                tail = f.read(1024 * 1024)
                if tail:
                    md5_hash.update(tail)
            except OSError:
                # Handle files smaller than 1 MB
                pass
    except FileNotFoundError:
        return None
    return md5_hash.hexdigest()

def make_dataframe_from_metadata(directory_path: str) -> pd.DataFrame:
    """
    Scan the specified directory for files and create a dataframe with metadata for each file.
    
    Args:
        directory_path (str): The directory containing the image files.
    
    Returns:
        pd.DataFrame: A dataframe containing metadata (filename, path, size, modified time, and hash) of the scanned files.
    """
    file_metadata_list = []
    start_time = time.time()

    def process_file(file_path: Path):
        try:
            file_stats = file_path.stat()
            modified_time = dt.datetime.fromtimestamp(file_stats.st_mtime).isoformat()
            size_kb = file_stats.st_size / 1024
            file_hash = calculate_file_hash(str(file_path))
            if file_hash:
                file_metadata_list.append([file_path.name, str(file_path), size_kb, modified_time, file_hash])
            if VERBOSE:
                elapsed_time = time.time() - start_time
                print(f'[PROGRESS] Processed {len(file_metadata_list)} files | Time elapsed: {elapsed_time:.2f}s')
        except (FileNotFoundError, PermissionError) as e:
            if VERBOSE:
                print(f'[ERROR] Failed to process file "{file_path}": {e}')

    # Use ThreadPoolExecutor to parallelize the file processing with more CPU cores for faster performance
    with concurrent.futures.ThreadPoolExecutor(max_workers=CPU_CORES) as executor:
        all_files = [Path(root) / filename for root, _, filenames in os.walk(directory_path) for filename in filenames if filename not in ('Thumbs.db', '.DS_Store','.processed_files.hash')]
        executor.map(process_file, all_files)

    return pd.DataFrame(file_metadata_list, columns=['Filename', 'Full_path', 'SizeKB', 'ModifiedTime', 'FileHash'])

def process_in_batches(df: pd.DataFrame, batch_size: int, process_function) -> None:
    """
    Process the dataframe in batches.
    
    Args:
        df (pd.DataFrame): The dataframe to be processed.
        batch_size (int): The number of rows to process in each batch.
        process_function (function): The function to apply to each batch.
    """
    for start_idx in range(0, len(df), batch_size):
        batch_df = df.iloc[start_idx:start_idx + batch_size]
        if VERBOSE:
            print(f'[INFO] Processing batch {start_idx // batch_size + 1}')
        process_function(batch_df)

def move_duplicate_files(dataframe: pd.DataFrame) -> None:
    """
    Identify and move duplicate files to a redundant directory.
    
    Args:
        dataframe (pd.DataFrame): The dataframe containing all file metadata.
    
    Moves:
        Redundant files to a directory named 'onepicture_deleteme'. If a file already exists in the destination, it is skipped.
    """
    redundant_directory = Path(REDUNDANT_DIRECTORY)
    redundant_directory.mkdir(parents=True, exist_ok=True)

    duplicates_df = dataframe[dataframe.duplicated(subset='FileHash', keep='first')]

    if VERBOSE:
        print(f'[INFO] Number of redundant files found: {len(duplicates_df)}')

    for idx, row in duplicates_df.iterrows():
        destination_path = redundant_directory / f"{idx}_{row['Filename']}"
        try:
            if VERBOSE:
                print(f'[ACTION] Moving "{row["Full_path"]}" to "{destination_path}"')
            if not destination_path.exists():
                shutil.copy2(row['Full_path'], destination_path)
        except (FileNotFoundError, PermissionError) as e:
            if VERBOSE:
                print(f'[ERROR] Failed to move file "{row["Full_path"]}": {e}')

def create_timeline_directories(unique_files_df: pd.DataFrame, timeline_directory: str, copied_files: int, skipped_files: int) -> (int, int):
    """
    Create directories based on a timeline (yyyy-mm) and move non-duplicate files accordingly.
    
    Args:
        unique_files_df (pd.DataFrame): The dataframe containing unique files.
        timeline_directory (str): The root directory for creating the timeline structure.
        copied_files (int): The number of files copied so far.
        skipped_files (int): The number of files skipped so far.
    
    Moves:
        Unique files to subdirectories organized by year and month of the modified time.
    """
    timeline_root = Path(timeline_directory)
    timeline_root.mkdir(parents=True, exist_ok=True)

    for _, row in unique_files_df.iterrows():
        modified_time = dt.datetime.fromisoformat(row['ModifiedTime'])
        year_month = modified_time.strftime('%Y-%m')
        destination_dir = timeline_root / year_month
        destination_path = destination_dir / row['Filename']

        try:
            if VERBOSE:
                print(f'[ACTION] Copying file from "{row["Full_path"]}" to "{destination_path}"')
            if not destination_path.exists():
                destination_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(row['Full_path'], destination_path)
                copied_files += 1
            else:
                skipped_files += 1
        except (FileNotFoundError, PermissionError) as e:
            if VERBOSE:
                print(f'[ERROR] Failed to move file "{row["Full_path"]}": {e}')

    return copied_files, skipped_files

if __name__ == "__main__":
    copied_files = 0
    skipped_files = 0
    if VERBOSE:
        print('[INFO] Program "onepicture" has started...')
    start_time = time.time()

    metadata_df = make_dataframe_from_metadata(PICTURE_DIRECTORY)
    if VERBOSE:
        print(metadata_df.head())
    unique_files_df = metadata_df.drop_duplicates(subset='FileHash')

    # Move duplicate files
    move_duplicate_files(metadata_df)
    # Process unique files for timeline directories in batches
    def process_batch(batch_df):
        global copied_files, skipped_files
        copied_files, skipped_files = create_timeline_directories(batch_df, TIMELINE_DIRECTORY, copied_files, skipped_files)

    process_in_batches(unique_files_df, BATCH_SIZE, process_batch)

    elapsed_time = time.time() - start_time
    if VERBOSE:
        print(f'[INFO] Program "onepicture" has ended. Total Elapsed Time: {elapsed_time:.2f}s')
        print(f'[INFO] Total unique files copied: {copied_files}, Total files skipped: {skipped_files}')
