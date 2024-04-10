import pandas as pd
import numpy as np
from functions import remove_last_col, print_to_csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait
from abstract_sample import AbstractSample, Trip, Ride, Station

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
    
def main():
    print("Start Reading. The process might take up to 3 minute...")
    with ThreadPoolExecutor() as executor:
        trip = Trip()
        ride = Ride()
        station = Station()
        print("Reading...")
        task_trips = executor.submit(process_files_with_processes, trip)
        task_rides = executor.submit(process_files_with_processes, ride)
        task_stations = executor.submit(process_files_with_processes, station)
        print("Waiting for reading task to complete...")
        trips_sample = task_trips.result()
        rides_sample = task_rides.result()
        station_sample = task_stations.result()

        task_stations = executor.submit(print_to_csv, station_sample, "stations.csv")
        print("Read")

        task_trips = executor.submit(remove_last_col, trips_sample)
        task_rides = executor.submit(remove_last_col, rides_sample)
        trips_sample = task_trips.result()
        rides_sample = task_rides.result()

        rides_sample = rides_sample[ride.selected_columns()]
        sample = pd.concat([trips_sample,rides_sample], ignore_index=True)
        #print
        task_sample = executor.submit(print_to_csv, sample, "sample.csv")
        wait([task_sample, task_stations])       
        print("Both processes completed.")

    print("All Done")

if __name__ == '__main__':
    main()