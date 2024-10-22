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
from tqdm import tqdm
import logging
import errno
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
logger = logging.getLogger(__name__)

# Directory paths 
PICTURE_DIRECTORY = Path(r'/Volumes/Master/All_PICTURES')
REDUNDANT_DIRECTORY = Path(r'/Volumes/DanExtDisk2/onepicture_deleteme')
TIMELINE_DIRECTORY = Path(r'/Volumes/DanExtDisk2/Photo_Time_line')
BATCH_SIZE = 1000
CHUNK_SIZE = 1024 * 1024

CPU_CORES = multiprocessing.cpu_count()  # Get the number of CPU cores available

def read_file_chunks(file_path: str, chunk_size: int) -> tuple[Optional[bytes], Optional[bytes]]:
    """
    Read the first and last chunk of the file.
    
    Args:
        file_path (str): The path to the file.
        chunk_size (int): The size of the chunk to read in bytes.
    
    Returns:
        tuple[Optional[bytes], Optional[bytes]]: The first and last chunk of the file, or None if the file cannot be found or read.
    """
    try:
        with open(file_path, "rb") as f:
            head = f.read(chunk_size)
            tail = None
            try:
                f.seek(-chunk_size, os.SEEK_END)
                tail = f.read(chunk_size)
            except OSError:
                # Handle files smaller than 1 MB
                pass
            return head, tail
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f'Failed to read file "%s": %s', file_path, e)
        return None, None

def calculate_file_hash(file_path: str) -> Optional[str]:
    """
    Calculate the MD5 hash of a file using the first and last chunk for faster processing.
    
    Args:
        file_path (str): The path to the file.
    
    Returns:
        Optional[str]: The MD5 hash of the file, or None if the file cannot be found or read.
    """
    md5_hash = hashlib.md5()
    head, tail = read_file_chunks(file_path, CHUNK_SIZE)
    if head:
        md5_hash.update(head)
    if tail:
        md5_hash.update(tail)
    return md5_hash.hexdigest() if head or tail else None

def process_file(file_path: Path) -> Optional[list]:
    """
    Process a file to extract its metadata and return it as a list.
    
    Args:
        file_path (Path): The path to the file to be processed.
    
    Returns:
        Optional[list]: A list containing the file's metadata or None if processing fails.
    """
    try:
        file_stats = file_path.stat()
        modified_time = dt.datetime.fromtimestamp(file_stats.st_mtime).isoformat()
        size_kb = file_stats.st_size / 1024
        file_hash = calculate_file_hash(str(file_path))
        file_extension = file_path.suffix.lower()
        if file_hash:
            return [file_path.name, str(file_path), size_kb, modified_time, file_hash, file_extension]
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f'Failed to process file "%s": %s', file_path, e)
    return None

def make_dataframe_from_metadata(directory_path: str) -> pd.DataFrame:
    """
    Scan the specified directory for files and create a dataframe with metadata for each file.
    This metadata includes filename, full path, size in KB, modified time, MD5 hash value, and file extension.
    
    Args:
        directory_path (str): The directory containing the image files.
    
    Returns:
        pd.DataFrame: A dataframe containing metadata (filename, path, size, modified time, hash, and file extension) of the scanned files.
    """
    file_metadata_list = []
    start_time = time.time()

    # Use ProcessPoolExecutor to parallelize the file processing for faster performance
    with concurrent.futures.ProcessPoolExecutor(max_workers=CPU_CORES) as executor:
        all_files = [Path(root) / filename for root, _, filenames in os.walk(directory_path) for filename in filenames if filename not in ('Thumbs.db', '.DS_Store','.processed_files.hash')]
        for metadata in tqdm(executor.map(process_file, all_files), total=len(all_files), desc="Processing files"):
            if metadata:
                file_metadata_list.append(metadata)

    elapsed_time = time.time() - start_time
    logger.info(f'Metadata extraction completed in %.2f seconds', elapsed_time)
    
    df = pd.DataFrame(file_metadata_list, columns=['Filename', 'Full_path', 'SizeKB', 'ModifiedTime', 'FileHash', 'FileExtension'])

    return df

def batch_generator(df: pd.DataFrame, batch_size: int):
    """
    Yield batches of the dataframe for processing in manageable chunks.
    
    Args:
        df (pd.DataFrame): The dataframe to be processed.
        batch_size (int): The number of rows in each batch.
    """
    for start_idx in range(0, len(df), batch_size):
        yield df.iloc[start_idx:start_idx + batch_size]

def identify_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Identify duplicate files based on their hash values.
    
    Args:
        dataframe (pd.DataFrame): The dataframe containing all file metadata.
    
    Returns:
        pd.DataFrame: A dataframe containing metadata of duplicate files.
    """
    duplicates_df = dataframe[dataframe.duplicated(subset='FileHash', keep='first')]
    logger.info(f'Number of redundant files found: %d', len(duplicates_df))
    return duplicates_df

def move_files(duplicates_df: pd.DataFrame, destination_directory: Path) -> None:
    """
    Move duplicate files to a specified redundant directory.
    Each duplicate file is renamed with a unique identifier to avoid filename collisions.
    
    Args:
        duplicates_df (pd.DataFrame): The dataframe containing metadata of duplicate files.
        destination_directory (Path): The directory to move redundant files to.
    """
    for idx, row in duplicates_df.iterrows():
        destination_path = destination_directory / f"{idx}_{row['Filename']}"
        try:
            logger.info(f'Moving "%s" to "%s"', row['Full_path'], destination_path)
            if not destination_path.exists():
                shutil.copy2(row['Full_path'], destination_path)
        except (FileNotFoundError, PermissionError) as e:
            logger.error(f'Failed to move file "%s": %s', row['Full_path'], e)
        except OSError as e:
            if e.errno == errno.ENOSPC:
                logger.error(f'Disk full while moving file "%s" to "%s": %s', row['Full_path'], destination_path, e)
            else:
                logger.error(f'OS error while moving file "%s" to "%s": %s', row['Full_path'], destination_path, e)

def create_timeline_directories(unique_files_df: pd.DataFrame, timeline_directory: str, copied_files: int, skipped_files: int) -> dict:
    """
    Create timeline-based directories (in yyyy-mm format) and move unique files accordingly.
    Files are organized by their modification date to help create a chronological file structure.
    
    Args:
        unique_files_df (pd.DataFrame): The dataframe containing unique files.
        timeline_directory (str): The root directory for creating the timeline structure.
        copied_files (int): The number of files copied so far.
        skipped_files (int): The number of files skipped so far.
    
    Returns:
        dict: A dictionary with updated counts of copied and skipped files.
    """
    timeline_root = create_directory(timeline_directory)

    for _, row in unique_files_df.iterrows():
        modified_time = dt.datetime.fromisoformat(row['ModifiedTime'])
        year_month = modified_time.strftime('%Y-%m')
        destination_dir = create_directory(timeline_root / year_month)
        destination_path = destination_dir / row['Filename']

        try:
            logger.info(f'Copying file from "%s" to "%s"', row['Full_path'], destination_path)
            if not destination_path.exists():
                shutil.copy2(row['Full_path'], destination_path)
                copied_files += 1
            else:
                skipped_files += 1
        except (FileNotFoundError, PermissionError) as e:
            logger.error(f'Failed to move file "%s": %s', row['Full_path'], e)
        except OSError as e:
            if e.errno == errno.ENOSPC:
                logger.error(f'Disk full while copying file "%s" to "%s": %s', row['Full_path'], destination_path, e)
            else:
                logger.error(f'OS error while copying file "%s" to "%s": %s', row['Full_path'], destination_path, e)

    return {'copied_files': copied_files, 'skipped_files': skipped_files}

def create_directory(path: Path) -> Path:
    """
    Create a directory if it does not exist.
    
    Args:
        path (Path): The path to the directory.
    
    Returns:
        Path: The created directory path.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path

def main():
    """
    Main function to manage the entire workflow of scanning, identifying duplicates, and organizing files.
    It starts by scanning a directory for image files, generates metadata, identifies duplicates, and then 
    organizes unique files into a timeline-based directory structure.
    """
    copied_files = 0
    skipped_files = 0
    logger.info('Program "onepicture" has started...')
    start_time = time.time()

    metadata_df = make_dataframe_from_metadata(PICTURE_DIRECTORY)
    logger.debug('First few rows of metadata dataframe:\n%s', metadata_df.head().to_string())
    unique_files_df = metadata_df.drop_duplicates(subset='FileHash')

    # Identify and move duplicate files
    duplicates_df = identify_duplicates(metadata_df)
    move_files(duplicates_df, create_directory(REDUNDANT_DIRECTORY))

    # Process unique files for timeline directories in batches
    for batch_df in batch_generator(unique_files_df, BATCH_SIZE):
        result = create_timeline_directories(batch_df, TIMELINE_DIRECTORY, copied_files, skipped_files)
        copied_files = result['copied_files']
        skipped_files = result['skipped_files']

    # Print file extension statistics
    extension_counts = Counter(metadata_df['FileExtension'])
    for ext, count in extension_counts.items():
        print(f'File extension: {ext}, Total number: {count}')

    elapsed_time = time.time() - start_time
    logger.info(f'Program "onepicture" has ended. Total Elapsed Time: %.2f seconds', elapsed_time)
    logger.info(f'Total unique files copied: %d, Total files skipped: %d', copied_files, skipped_files)

if __name__ == "__main__":
    main()
