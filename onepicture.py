#!/usr/bin/python3
# -*- coding: utf-8 -*-
#===============================================================================
# Dan Zhao
# 10-22-2024
#===============================================================================
# This program scans a directory for image files, creates metadata, identifies duplicate files, 
# and moves redundant files to a specified directory.
# Duplicate files are identified based on their hash values to ensure content-based matching.
# This code was improved and refactored with the assistance of OpenAI's language model, ChatGPT-4, to enhance readability and efficiency.
import os
import datetime as dt
import pandas as pd
import hashlib
from pathlib import Path

# Directory paths 
PICTURE_DIRECTORY = r'D:/ALL_PICTURES'
REDUNDANT_DIRECTORY = r'D:/onepicture_deleteme'
TIMELINE_DIRECTORY = r'D:/Photo_Timeline'

def calculate_file_hash(file_path: str) -> str | None:
    """
    Calculate the SHA-256 hash of a file.
    
    Args:
        file_path (str): The path to the file.
    
    Returns:
        str | None: The SHA-256 hash of the file, or None if the file cannot be found.
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
    except FileNotFoundError:
        return None
    return sha256_hash.hexdigest()

def make_dataframe_from_metadata(directory_path: str) -> pd.DataFrame:
    """
    Scan the specified directory for files and create a dataframe with metadata for each file.
    
    Args:
        directory_path (str): The directory containing the image files.
    
    Returns:
        pd.DataFrame: A dataframe containing metadata (filename, path, size, modified time, and hash) of the scanned files.
    """
    file_metadata_list = []
    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            full_path = Path(root) / filename
            try:
                file_stats = full_path.stat()
                modified_time = dt.datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                size_kb = file_stats.st_size / 1024
                file_hash = calculate_file_hash(str(full_path))
                if file_hash:
                    file_metadata_list.append([filename, str(full_path), size_kb, modified_time, file_hash])
            except (FileNotFoundError, PermissionError):
                print(f'[WARNING] Skipping file "{full_path}" due to missing permissions or file not found.')
                continue

    return pd.DataFrame(file_metadata_list, columns=['Filename', 'Full_path', 'SizeKB', 'ModifiedTime', 'FileHash'])

def move_duplicate_files(dataframe: pd.DataFrame) -> None:
    """
    Identify and move duplicate files to a redundant directory.
    
    Args:
        dataframe (pd.DataFrame): The dataframe containing all file metadata.
    
    Moves:
        Redundant files to a directory named 'onepicture_deleteme'.
    """
    redundant_directory = Path(REDUNDANT_DIRECTORY)
    redundant_directory.mkdir(parents=True, exist_ok=True)

    unique_files_df = dataframe.drop_duplicates(subset='FileHash')
    duplicates_df = dataframe.loc[~dataframe.index.isin(unique_files_df.index)]
    duplicates_df = duplicates_df[duplicates_df['Filename'] != 'Thumbs.db']

    print(f'[INFO] Number of redundant files found: {len(duplicates_df.index)}')
    for idx, row in duplicates_df.iterrows():
        destination_path = redundant_directory / f"{idx}_{row['Filename']}"
        try:
            print(f'[ACTION] Moving file from "{row['Full_path']}" to "{destination_path}"')
            Path(row['Full_path']).rename(destination_path)
        except (FileNotFoundError, PermissionError) as e:
            print(f'[ERROR] Failed to move file "{row['Full_path']}": {e}')

def create_timeline_directories(unique_files_df: pd.DataFrame, timeline_directory: str) -> None:
    """
    Create directories based on a timeline (yyyy-mm) and move non-duplicate files accordingly.
    
    Args:
        unique_files_df (pd.DataFrame): The dataframe containing unique files.
        timeline_directory (str): The root directory for creating the timeline structure.
    
    Moves:
        Unique files to subdirectories organized by year and month of the modified time.
    """
    timeline_root = Path(timeline_directory)
    timeline_root.mkdir(parents=True, exist_ok=True)

    for _, row in unique_files_df.iterrows():
        modified_time = dt.datetime.fromisoformat(row['ModifiedTime'])
        year_month = modified_time.strftime('%Y-%m')
        destination_dir = timeline_root / year_month
        hash_file_path = destination_dir / ".processed_files.hash"

        # Read existing hashes from the .hash file
        processed_hashes = set()
        if hash_file_path.exists():
            with open(hash_file_path, "r") as hash_file:
                processed_hashes = {line.strip() for line in hash_file}

        # Check if the file has already been processed
        if row['FileHash'] in processed_hashes:
            print(f'[INFO] File "{row['Filename']}" already exists in the timeline directory, skipping.')
            continue

        destination_dir.mkdir(parents=True, exist_ok=True)
        destination_path = destination_dir / row['Filename']

        try:
            print(f'[ACTION] Moving file from "{row['Full_path']}" to "{destination_path}"')
            Path(row['Full_path']).rename(destination_path)
            # Update the hash file with the new hash
            with open(hash_file_path, "a") as hash_file:
                hash_file.write(f"{row['FileHash']}\n")
        except (FileNotFoundError, PermissionError) as e:
            print(f'[ERROR] Failed to move file "{row['Full_path']}": {e}')

if __name__ == "__main__":
    print('[INFO] Program "onepicture" has started...')

    metadata_df = make_dataframe_from_metadata(PICTURE_DIRECTORY)
    unique_files_df = metadata_df.drop_duplicates(subset='FileHash')
    move_duplicate_files(metadata_df)
    create_timeline_directories(unique_files_df, TIMELINE_DIRECTORY)

    print('[INFO] Program "onepicture" has ended.')
