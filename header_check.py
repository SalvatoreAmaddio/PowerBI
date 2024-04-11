import pandas as pd
from abstract_sample import Station, Trip, Ride

station = Station()
trips = Trip()
rides = Ride()

files = station.paths

def loop(files:list)->pd.DataFrame:
    dataset = pd.DataFrame()
    for path in files:
        headers = pd.read_csv(path, nrows=0)
        list = []

        for column in headers.columns:
            list.append(column)

        df = pd.DataFrame([list])
        dataset = pd.concat([dataset,df], ignore_index=True)
    return dataset

loop(station.paths).to_csv("station_headers_check.csv", index=False)
loop(trips.paths).to_csv("trips_headers_check.csv", index=False)
loop(rides.paths).to_csv("rides_headers_check.csv", index=False)


