"""
This script produces three CSV files.
Each file list headers of from all files.
This is to check incosistent column naming across files and 
potential inconsistency of number of columns.
"""
import pandas as pd
from abstract_sample import Station, Trip, Ride

station = Station()
trips = Trip()
rides = Ride()

def loop_through(files:list)->pd.DataFrame:
    dataset = pd.DataFrame()
    for path in files:
        headers = pd.read_csv(path, nrows=0)
        list = []

        for column in headers.columns:
            list.append(column)

        df = pd.DataFrame([list])
        dataset = pd.concat([dataset,df], ignore_index=True)
    return dataset

loop_through(station.paths).to_csv("station_headers_check.csv", index=False)
loop_through(trips.paths).to_csv("trips_headers_check.csv", index=False)
loop_through(rides.paths).to_csv("rides_headers_check.csv", index=False)


