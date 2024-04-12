import pandas as pd
from concurrent.futures import ThreadPoolExecutor, wait
from scripts.functions import *
from scripts.abstract_sample import Trip, Ride, Station
    
def main():
    print("Start Reading. The process might take up to 3 minute...")
    with ThreadPoolExecutor() as executor:
        trip = Trip()
        ride = Ride()
        station = Station()
        print("Reading...")
        #read the directories at the same time.
        task_trips = executor.submit(process_files_with_processes, trip)
        task_rides = executor.submit(process_files_with_processes, ride)
        task_stations = executor.submit(process_files_with_processes, station)
        print("Waiting for reading task to complete. Go grab a coffee...")
        trips_sample = task_trips.result()
        rides_sample = task_rides.result()
        station_sample = task_stations.result()
        print("All Read")

        station_sample = station_sample[station.selected_columns()]
        print("Printing stations...")        
        task_stations = executor.submit(print_to_csv, station_sample, "processed_data\stations.csv")

        print("Adjusting columns...")        
        task_trips = executor.submit(remove_last_col, trips_sample)
        task_rides = executor.submit(remove_last_col, rides_sample)
        trips_sample = task_trips.result()
        rides_sample = task_rides.result()
        rides_sample = rides_sample[ride.selected_columns()]

        #put the datasets from Rides and Trips into one        
        sample = pd.concat([trips_sample,rides_sample], ignore_index=True)

        #print data_trips
        task_sample = executor.submit(print_to_csv, sample, "processed_data\data_trips.csv")
        
        #ensure both data_trips and stations are printed before terminating the program.
        wait([task_sample, task_stations])       
        print("Both processes completed.")

    print("All Done")

if __name__ == '__main__':
    main()