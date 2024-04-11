import pandas as pd
from functions import ProcessPoolExecutor, as_completed
from abstract_sample import AbstractSample

def remove_last_col(sample:pd.DataFrame)->pd.DataFrame:
    return sample.drop(sample.columns[-1], axis=1)

def print_to_csv(sample:pd.DataFrame, file_name:str):
    sample.to_csv(file_name, index=False)

# Function to process files using ProcessPoolExecutor  file_paths, dir:int = 0
def process_files_with_processes(abstract_sample:AbstractSample):
    samples = []
    with ProcessPoolExecutor() as executor:
        # Submit all the file paths for processing
        tasks = [executor.submit(abstract_sample.extract_sample, path) for path in abstract_sample.paths]

        for task in as_completed(tasks):
            try:
                # Retrieve the result and append it to the samples list
                data = task.result()
                samples.append(data)
            except Exception as exc:
                print(f'Generated an exception: {exc}')
    return pd.concat(samples, ignore_index=True)
