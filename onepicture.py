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
import time

# Directory paths 
PICTURE_DIRECTORY = r'/Volumes/DanExtDisk2/ALL_PICTURES'
REDUNDANT_DIRECTORY = r'/Volumes/DanExtDisk2/onepicture_deleteme'
TIMELINE_DIRECTORY = r'/Volumes/DanExtDisk2/photo_Timeline'
BATCH_SIZE = 100

def calculate_file_hash(file_path: str) -> Optional[str]:
    """
    Calculate the MD5 hash of a file using the first and last 1 MB for faster processing. This approach reduces the time needed for large files.
    
    Args:
        file_path (str): The path to the file.
    
    Returns:
        Optional[str]: The MD5 hash of the file, or None if the file cannot be found.
    """
    md5_hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # Hash the first and last 1 MB of the file for faster processing
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
    file_metadata_list = []  # Store metadata for all files
    total_files = sum(len(files) for _, _, files in os.walk(directory_path))  # Get the total number of files to process
    processed_files = 0  # Counter for processed files
    start_time = time.time()  # Record the start time for progress tracking

    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            if filename == 'Thumbs.db':
                continue  # Skip Thumbs.db files
            full_path = Path(root) / filename
            try:
                file_stats = full_path.stat()
                modified_time = dt.datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                size_kb = file_stats.st_size / 1024
                file_hash = calculate_file_hash(str(full_path))
                if file_hash:
                    file_metadata_list.append([filename, str(full_path), size_kb, modified_time, file_hash])
                processed_files += 1  # Update counter after processing each file
                elapsed_time = time.time() - start_time
                print(f'[PROGRESS] {processed_files}/{total_files} files processed | Time elapsed: {elapsed_time:.2f}s')
            except (FileNotFoundError, PermissionError) as e:
                print(f'[ERROR] Failed to process file "{full_path}": {e}')
                print(f'[WARNING] Skipping file "{full_path}" due to missing permissions or file not found.')
                continue

    return pd.DataFrame(file_metadata_list, columns=['Filename', 'Full_path', 'SizeKB', 'ModifiedTime', 'FileHash'])

def process_in_batches(df: pd.DataFrame, batch_size: int, process_function) -> None:
    """
    Process the dataframe in batches.
    
    Args:
        df (pd.DataFrame): The dataframe to be processed.
        batch_size (int): The number of rows to process in each batch.
        process_function (function): The function to apply to each batch.
    """
    total_rows = len(df)  # Total number of rows in the dataframe
    start_time = time.time()
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        print(f'[INFO] Processing batch {start_idx // batch_size + 1} ({start_idx} to {end_idx} of {total_rows})')
        process_function(batch_df)
        elapsed_time = time.time() - start_time
        print(f'[INFO] Completed batch {start_idx // batch_size + 1} | Elapsed Time: {elapsed_time:.2f}s')

def move_duplicate_files(dataframe: pd.DataFrame) -> None:
    """
    Identify and move duplicate files to a redundant directory. Uses MD5 hash to detect duplicates.
    
    Args:
        dataframe (pd.DataFrame): The dataframe containing all file metadata.
    
    Moves:
        Redundant files to a directory named 'onepicture_deleteme'. If a file already exists in the destination, it is skipped.
    """
    redundant_directory = Path(REDUNDANT_DIRECTORY)
    redundant_directory.mkdir(parents=True, exist_ok=True)

    duplicates_df = dataframe[dataframe.duplicated(subset='FileHash', keep='first')]  # Identify duplicate files based on hash
    

    print(f'[INFO] Number of redundant files found: {len(duplicates_df.index)}')
    start_time = time.time()
    for idx, row in duplicates_df.iterrows():
        destination_path = redundant_directory / f"{idx}_{row['Filename']}"
        try:
            print(f'[ACTION] Moving "{row["Full_path"]}" to "{destination_path}"')
            if not destination_path.exists():
                Path(row['Full_path']).rename(destination_path)
            else:
                print(f'[INFO] Skipping existing file: "{destination_path}"')
        except (FileNotFoundError, PermissionError) as e:
            print(f'[ERROR] Failed to move file "{row["Full_path"]}": {e.strerror}')
    elapsed_time = time.time() - start_time
    print(f'[INFO] Completed moving redundant files | Elapsed Time: {elapsed_time:.2f}s')

def create_timeline_directories(unique_files_df: pd.DataFrame, timeline_directory: str) -> None:
    """
    Create directories based on a timeline (yyyy-mm) and move non-duplicate files accordingly. Uses MD5 hash for duplicate detection, and avoids overwriting existing files.
    
    Args:
        unique_files_df (pd.DataFrame): The dataframe containing unique files.
        timeline_directory (str): The root directory for creating the timeline structure.
    
    Moves:
        Unique files to subdirectories organized by year and month of the modified time.
    """
    timeline_root = Path(timeline_directory)
    timeline_root.mkdir(parents=True, exist_ok=True)

    total_files = len(unique_files_df)
    processed_files = 0
    start_time = time.time()

    for _, row in unique_files_df.iterrows():
        row = row.copy()
        modified_time = dt.datetime.fromisoformat(row['ModifiedTime'])
        year_month = modified_time.strftime('%Y-%m')
        destination_dir = timeline_root / year_month
        hash_file_path = destination_dir / ".processed_files.hash"

        # Read existing hashes from the .hash file
        processed_hashes = set()
        if hash_file_path.exists():
            with open(hash_file_path, "r") as hash_file:
                processed_hashes = set(line.strip() for line in hash_file)  # Read hashes of already processed files

        # Check if the file has already been processed
        if row['FileHash'] in processed_hashes:
            print(f'[INFO] Skipping already processed file: "{row["Filename"]}"')
            continue

        destination_dir.mkdir(parents=True, exist_ok=True)
        destination_path = destination_dir / row['Filename']

        try:
            print(f'[ACTION] Moving file from "{row["Full_path"]}" to "{destination_path}"')
            if not destination_path.exists():
                Path(row['Full_path']).rename(destination_path)
            else:
                print(f'[INFO] File "{destination_path}" already exists, skipping.')
            # Update the hash file with the new hash
            with open(hash_file_path, "a") as hash_file:
                hash_file.write(f"{row['FileHash']}")
        except (FileNotFoundError, PermissionError) as e:
            print(f'[ERROR] Failed to move file "{row["Full_path"]}": {e}')
        processed_files += 1
        elapsed_time = time.time() - start_time
        print(f'[PROGRESS] {processed_files}/{total_files} unique files processed | Time elapsed: {elapsed_time:.2f}s')

if __name__ == "__main__":
    print('[INFO] Program "onepicture" has started...')
    start_time = time.time()

    metadata_df = make_dataframe_from_metadata(PICTURE_DIRECTORY)
    print(metadata_df.head())
    unique_files_df = metadata_df.drop_duplicates(subset='FileHash')

    # Move duplicate files (processed globally)
    move_duplicate_files(metadata_df)
    # Process unique files for timeline directories in batches
    process_in_batches(unique_files_df, BATCH_SIZE, lambda batch: create_timeline_directories(batch, TIMELINE_DIRECTORY))

    elapsed_time = time.time() - start_time
    print(f'[INFO] Program "onepicture" has ended. Total Elapsed Time: {elapsed_time:.2f}s')
