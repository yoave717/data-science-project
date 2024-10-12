import uuid
import concurrent
import pandas as pd
from datetime import date, timedelta
import glob
import os
import re
import zipfile
import requests


def daterange(start_date, end_date):
    """
    Generate dates from start_date to end_date (exclusive).

    Args:
        start_date (datetime.date): The start date.
        end_date (datetime.date): The end date.

    Yields:
        datetime.date: The next date in the range.
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


# The function iterates over each day and hour in the year, constructs the corresponding file URL,
#  and sends a GET request to download the file. If the file is successfully downloaded,
#  it is saved in the 'compressed' directory and then extracted to the 'data' directory.

# If the file is not found on the server, its URL is added to a list of missing files,
#  which is printed at the end of the function.

# The function also reads and writes the last downloaded date and hour from/to a file,
#  allowing the download process to be resumed if was interrupted.

def download_files(year, data_folder):
    """
    Download files for each hour of each day in the specified year.

    Args:
        year (int): The year for which to download files.
        data_folder (str): The folder where data will be stored.

    Returns:
        None
    """ 
    start_date = date(year, 1, 1)
    end_date = date(year+1, 1, 1)
    missing_files = []

    if not os.path.exists(f'{data_folder}/compressed'):
        os.makedirs(f'{data_folder}/compressed')
    if not os.path.exists(f'{data_folder}/data'):
        os.makedirs(f'{data_folder}/data')

    # Read the start date and hour from a file
    try:
        with open(f'{data_folder}/last_downloaded.txt', 'r') as f:
            last_downloaded = f.read().strip()
            start_date = date(int(last_downloaded[:4]), int(last_downloaded[5:7]), int(last_downloaded[8:10]))
            start_hour = int(last_downloaded[11:13])
    except FileNotFoundError:
        start_hour = 0

    for single_date in daterange(start_date, end_date):
        for hour in range(start_hour, 24):
            filename = f"{single_date.strftime('%Y-%m-%d')}.{str(hour).zfill(2)}"
            url = f"https://s3.eu-west-2.wasabisys.com/stride/stride-etl-packages/siri/{single_date.strftime('%Y/%m')}/{filename}.zip"
            response = requests.get(url)
            if response.status_code == 200:
                with open(f"{data_folder}/compressed/{filename}.zip", 'wb') as f:
                    f.write(response.content)
                with zipfile.ZipFile(f"{data_folder}/compressed/{filename}.zip", 'r') as zip_ref:
                    if f"{filename}.csv" in zip_ref.namelist():
                        zip_ref.extract(f"{filename}.csv", path=f'{data_folder}/data')
                # Save the current date and hour to a file
                with open(f'{data_folder}/last_downloaded.txt', 'w') as f:
                    f.write(f"{single_date.strftime('%Y-%m-%d')}.{str(hour).zfill(2)}")
            else:
                missing_files.append(url)
        start_hour = 0

    print("Missing files:")
    for file in missing_files:
        print(file)


def extract_missing_csv(data_folder):
    """
    Extract missing CSV files from ZIP archives in the specified data folder.

    Args:
        data_folder (str): The folder where compressed ZIP files and extracted CSV files are stored.

    Returns:
        None
    """
    zip_dir = f'{data_folder}/compressed'
    csv_dir = f'{data_folder}/data'
    # Get a list of all ZIP files
    zip_files = glob.glob(f'{zip_dir}/*.zip')
    zip_files.sort()

    for zip_file in zip_files:
        # Get the corresponding CSV file name
        csv_file_name = os.path.basename(zip_file)[:-4] + '.csv'
        csv_file_path = f'{csv_dir}/{csv_file_name}'
        
        # If the CSV file does not exist
        if not os.path.exists(csv_file_path):
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Extract the CSV file
                if csv_file_name in zip_ref.namelist():
                    zip_ref.extract(csv_file_name, path=csv_dir)

def get_folder_size(folder_path):
    """
    Calculate the total size of all files in a given folder.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        int: The total size of all files in the folder in bytes.
    """
    total = 0
    # Walk through all directories and files in the given folder path
    for path, dirs, files in os.walk(folder_path):
        for f in files:
            # Construct the full file path
            fp = os.path.join(path, f)
            # Add the size of the file to the total
            total += os.path.getsize(fp)
    return total

import re

def extract_number(file_name):
    """
    Extract a number from a file name using a specific pattern.

    Args:
        file_name (str): The name of the file.

    Returns:
        int: The extracted number if the pattern matches, otherwise 0.
    """
    match = re.search(r'output_(\d+)_from_(.+)_at_(\d+)_to_(.+)_at_(-?\d+)', file_name)
    return int(match.group(1)) if match else 0

def extract_variables(file_name):
    """
    Extract multiple variables from a file name using a specific pattern.

    Args:
        file_name (str): The name of the file.

    Returns:
        tuple: A tuple containing the extracted variables (x, start_file, start_pos, end_file, end_pos) if the pattern matches, otherwise None.
    """
    match = re.search(r'output_(\d+)_from_(.+)_at_(\d+)_to_(.+)_at_(\d+)', file_name)
    if match:
        x = int(match.group(1))
        start_file = match.group(2)
        start_pos = int(match.group(3))
        end_file = match.group(4)
        end_pos = int(match.group(5))
        return x, start_file, start_pos, end_file, end_pos
    else:
        return None

def save_df_to_parquet(df, file_counter, start_file, start_pos, last_file, end_pos, files_to_remove, location):
    """
    Save a DataFrame to a Parquet file with a specific naming convention and remove processed CSV files.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        file_counter (int): The counter for the output file name.
        start_file (str): The name of the starting file.
        start_pos (int): The starting position in the starting file.
        last_file (str): The name of the last file.
        end_pos (int): The ending position in the last file.
        files_to_remove (list): A list of file paths to remove after processing.
        location (str): The directory where the Parquet file will be saved.

    Returns:
        None
    """
    print(f'Saving file {file_counter}...')
    
    dtypes = {
        'id': 'string',
        'bearing': 'int32',
        'lat': 'float64',
        'lon': 'float64',
        'gtfs_stop_lat': 'float64',
        'gtfs_stop_lon': 'float64',
        }
    
    date_cols = ['recorded_at_time', 'siri_scheduled_start_time', 'gtfs_start_time', 'gtfs_end_time', 'gtfs_arrival_time', 'gtfs_departure_time']
    
    # converting types
    df = df.astype(dtypes)
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%S%z')
    
    start_file_name = os.path.splitext(os.path.basename(start_file))[0]
    last_file_name = os.path.splitext(os.path.basename(last_file))[0]

    file_name = f'{location}/output_{file_counter}_from_{start_file_name}_at_{start_pos}_to_{last_file_name}_at_{end_pos}.parquet'

    df.to_parquet(file_name, index=False)

    # remove csv files
    print(f'Processed files {os.path.basename(files_to_remove[0])} to {os.path.basename(files_to_remove[-2])}. Now Deleting...')
    while len(files_to_remove) > 1:
        os.remove(files_to_remove[0])
        files_to_remove.pop(0)

    if end_pos == -1 and files_to_remove:
        os.remove(files_to_remove[0])
        files_to_remove.pop(0)

def process_files(folder_path, rows_per_file=10000000):
    """
    Process CSV files in a folder, concatenate them into a DataFrame, and save the DataFrame to Parquet files.

    Args:
        folder_path (str): The path to the folder containing the CSV files.
        rows_per_file (int, optional): The maximum number of rows per output Parquet file. Defaults to 10,000,000.

    Returns:
        None
    """
    print(f"Folder size before processing: {get_folder_size(folder_path)} bytes")

    output_files_folder_path = f'{folder_path}\\concatenated_data_parquet\\'

    df = pd.DataFrame()

    columns_to_drop = ['gtfs_agency_name', 'gtfs_stop_name', 'gtfs_route_long_name', 'gtfs_line_ref', 'gtfs_operator_ref', 'distance_from_siri_ride_stop_meters', 'distance_from_journey_start']

    start_file = None
    start_pos = None
    last_file = None
    end_pos = None

    csv_files = glob.glob(f'{folder_path}\\data\\*.csv')

    csv_files.sort()

    file_counter = 1

    output_files = glob.glob(f'{output_files_folder_path}output_*.parquet')

    if output_files:
        last_output_file = max(output_files, key=extract_number)
        file_counter, start_file, start_pos, last_file, end_pos = extract_variables(last_output_file)
        last_df = pd.read_parquet(last_output_file)
        # If the last output file contains less than max rows, load it into df
        if len(last_df) < rows_per_file:
            df = last_df
            os.remove(last_output_file)  # remove the last file as it will be rewritten later
        else:
            start_file = last_file
            start_pos = end_pos
            file_counter += 1
    
    if last_file is not None:
        start_index = csv_files.index(f'{folder_path}\\data\\{last_file}.csv')
    else:
        start_index = 0
        
    files_to_remove = []
    for file in csv_files[start_index:]:
        files_to_remove.append(file)
        # If the file is not empty
        if os.path.getsize(file) > 0:
            print(f'On file {file}')
            try:
                if end_pos is None:
                    end_pos = 0
                if start_file is None:
                    start_file = file

                
                if csv_files.index(file) == start_index:
                    temp_df = pd.read_csv(file, dtype='string', skiprows=range(1, end_pos))
                else:
                    temp_df = pd.read_csv(file, dtype='string')
                print(temp_df.shape[0])
                temp_df['original_file'] = os.path.basename(file)  # Add the original file name to each row
                last_file = file
                
                # Remove duplicates
                temp_df = temp_df.drop_duplicates()

                # Drop the unnecessary columns
                temp_df = temp_df.drop(columns=columns_to_drop)
                df = pd.concat([df, temp_df])
                
                # If the main DataFrame has reached max rows
                print(df.shape[0], temp_df.shape[0])
                if df.shape[0] >= rows_per_file:
                    start_pos = end_pos
                    end_pos = df.shape[0] - rows_per_file
                    save_df_to_parquet(df[:rows_per_file], file_counter, start_file, start_pos, last_file, end_pos, files_to_remove, output_files_folder_path)

                    # Keep the remaining rows in the DataFrame
                    df = df[rows_per_file:]
                    file_counter += 1
                    start_file = file

            except pd.errors.EmptyDataError:
                print(f"File {file} is empty or only contains a header.")
        
    # Write the remaining rows in the DataFrame to a parquet file
    if not df.empty:
        save_df_to_parquet(df, file_counter, start_file, start_pos, last_file, -1, files_to_remove, output_files_folder_path)

    print(f"Folder size after processing: {get_folder_size(folder_path)} bytes")
    
    
def time_features_extraction(results):
    """
    Extracts time-based features from datetime columns in the DataFrame.
    
    Parameters:
    results (pd.DataFrame): DataFrame containing datetime columns to extract features from.
    
    Returns:
    pd.DataFrame: DataFrame with new columns for extracted time features.
    """

    results['gtfs_start_time__day_of_week'] = results['gtfs_start_time'].dt.dayofweek
    results['gtfs_start_time__hour'] = results['gtfs_start_time'].dt.hour

    results['gtfs_end_time__day_of_week'] = results['gtfs_end_time'].dt.dayofweek
    results['gtfs_end_time__hour'] = results['gtfs_end_time'].dt.hour

    results['recorded_at_time__day_of_week'] = results['recorded_at_time'].dt.dayofweek
    results['recorded_at_time__hour'] = results['recorded_at_time'].dt.hour
    results['recorded_at_time__minute'] = results['recorded_at_time'].dt.minute
    results['recorded_at_time__day_of_month'] = results['recorded_at_time'].dt.day
    results['recorded_at_time__month'] = results['recorded_at_time'].dt.month

    results['estimated_arrival_time__day_of_week'] = results['estimated_arrival_time'].dt.dayofweek
    results['estimated_arrival_time__hour'] = results['estimated_arrival_time'].dt.hour
    results['estimated_arrival_time__minute'] = results['estimated_arrival_time'].dt.minute
    results['estimated_arrival_time__second'] = results['estimated_arrival_time'].dt.second
    results['estimated_arrival_time__day_of_month'] = results['estimated_arrival_time'].dt.day
    results['estimated_arrival_time__month'] = results['estimated_arrival_time'].dt.month
    
    results['scheduled_arrival_time__day_of_week'] = results['scheduled_arrival_time'].dt.dayofweek
    results['scheduled_arrival_time__hour'] = results['scheduled_arrival_time'].dt.hour
    results['scheduled_arrival_time__minute'] = results['scheduled_arrival_time'].dt.minute
    results['scheduled_arrival_time__second'] = results['scheduled_arrival_time'].dt.second
    results['scheduled_arrival_time__day_of_month'] = results['scheduled_arrival_time'].dt.day
    results['scheduled_arrival_time__month'] = results['scheduled_arrival_time'].dt.month
    
    return results


# Batch Creation Function

# the function `create_batch_df` to create data batches from the large dataset. The function takes the following parameters:

# - `csv_dir`: The directory where the CSV files are stored.
# - `batch_size_gb`: The maximum size of each batch in gigabytes.
# - `last_journey_refs`: A set of 'siri_journey_ref' values that were included in the last batch and should be excluded from the current batch.
# - `start_file`: The file to start reading from. If not specified, reading starts from the first file in the directory.
# - `start_pos`: The position in the start file to start reading from.

# The function works as follows:

# 1. It initializes an empty DataFrame for the batch and a set to keep track of the 'siri_journey_ref' values in the current batch.

# 2. It iterates over each file in the directory, starting from the `start_file` if specified.

# 3. For each file, it creates an iterator for the chunks in the file and reads the first chunk.

# 4. If there are any `last_journey_refs`, it excludes them from the chunk.

# 5. It then enters a loop where it adds the chunk to the batch if it doesn't exceed the `batch_size_gb`. If the batch size is exceeded, it adds only the rows with the same 'siri_journey_ref' as in the current batch.

# 6. The function returns the batch DataFrame, the set of 'siri_journey_ref' values in the current batch, the next chunk, the iterator for the next chunks, the next file, and the index of the next file.

# By using this function, we can create manageable batches from the large dataset while ensuring that all related lines are included in the same batch.



def create_batch_df(parquet_dir, batch_size_gb, last_batch_journey_refs=None, start_file=None, start_pos=0):
    # Convert batch size from GB to bytes
    batch_size_bytes = batch_size_gb * 1024 * 1024 * 1024

    # Get a list of all output Parquet files
    parquet_files = glob.glob(f'{parquet_dir}/output_*.parquet')
    parquet_files.sort(key=extract_number)

    # If a start file is specified, start from this file
    if start_file is not None:
        parquet_files = parquet_files[parquet_files.index(start_file):]

    # Initialize an empty DataFrame for the batch
    batch_df = pd.DataFrame()

    # Initialize a set to keep track of the 'siri_journey_ref' values in the current batch
    current_journey_refs = set()

    for file in parquet_files:
        # Create an iterator for the chunks in the Parquet file
        parquet_file = pq.ParquetFile(file)
        chunk_iter = parquet_file.iter_batches(batch_size=100000)  # Adjust batch size as needed

        # Read the first chunk and convert it to a DataFrame
        chunk = pd.DataFrame(next(chunk_iter).to_pandas())

        # If there are any last journey refs, exclude them from the chunk
        if last_batch_journey_refs is not None:
            chunk = chunk[~chunk['siri_journey_ref'].isin(last_batch_journey_refs)]
        while True:
            # Add the chunk to the batch if it doesn't exceed the batch size
            if (batch_df.memory_usage(index=True, deep=True).sum() + chunk.memory_usage(index=True, deep=True).sum()) <= batch_size_bytes:
                batch_df = pd.concat([batch_df, chunk])
                current_journey_refs.update(chunk['siri_journey_ref'].unique())

            else:
                # If the batch size is exceeded, add only the rows with the same 'siri_journey_ref' as in the current batch
                current_journey_refs_in_chunk = chunk['siri_journey_ref'].isin(current_journey_refs)
                if current_journey_refs_in_chunk.any():
                    batch_df = pd.concat([batch_df, chunk[current_journey_refs_in_chunk]])
                    chunk = chunk[~current_journey_refs_in_chunk]

                # Return the current DataFrame and the set of 'siri_journey_ref' values
                return batch_df, current_journey_refs, chunk, chunk_iter, file, parquet_files.index(file)

            # Try to read the next chunk
            try:
                chunk = pd.DataFrame(next(chunk_iter).to_pandas())
            except StopIteration:
                break

    # If all files have been processed, return the current DataFrame and the set of 'siri_journey_ref' values
    return batch_df, current_journey_refs, None, None, None, None
