import pandas as pd
from abstract_sample import Ride


rides = Ride()

def loop_through(files:list)->pd.DataFrame:
    dataset = pd.DataFrame()
    for path in files:
        bikes = pd.read_csv(path, usecols="rideable_type")
        bikes = bikes.drop_duplicates()
        dataset = pd.concat([dataset,bikes], ignore_index=True)
    return dataset


loop_through(rides.paths).to_csv("all_bikes.csv", index=False)