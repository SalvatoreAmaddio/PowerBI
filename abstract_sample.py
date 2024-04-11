import os
from abc import ABC, abstractmethod
import pandas as pd

class AbstractSample:

    new_column_names = []

    def __init__(self, dir:str):
        self._dir = dir
        self._paths = self.directory_files()

    @property
    def paths(self):
        return self._paths

    def directory_files(self):
        directory_files = os.listdir(self._dir)
        csv_files = [file for file in directory_files if file.endswith('.csv')]
        return [os.path.join(self._dir, file) for file in csv_files]

    @abstractmethod
    def rename_columns_as(self, column_count:int=0):
        pass

    @abstractmethod
    def selected_columns(self)->list:
        pass
    
    def extract_sample(self, path: str)-> pd.DataFrame:
        df_header = pd.read_csv(path, nrows=0)
        column_count = len(df_header.columns)
        self.rename_columns_as(column_count)
        data_set = pd.read_csv(path, names=self.new_column_names, header=0, usecols = self.selected_columns())    
        data_set['startTime'] = pd.to_datetime(data_set['startTime'], errors='coerce')
        data_set['stopTime'] = pd.to_datetime(data_set['stopTime'], errors='coerce')
        data_set['month'] = data_set['startTime'].dt.month
        return data_set.groupby('month').head(500)

class Trip(AbstractSample):

    def __init__(self):
        super().__init__("trips/")

    def rename_columns_as(self, column_count:int=0):
        self.new_column_names = ['tripID', 'startTime', 'stopTime', 'bike', 'tripDuration', 'fromStationID', 'fromStationName', 'toStationID', 'toStationName', 'userType', 'gender', 'birthYear']
    
    def selected_columns(self)->list:
        return ['startTime', 'stopTime', 'bike', 'fromStationID', 'toStationID', 'userType', 'gender', 'birthYear']


class Ride(AbstractSample):

    def __init__(self):
        super().__init__("rides/")

    def rename_columns_as(self, column_count:int=0):
        self.new_column_names = ['tripID', 'bike', 'startTime', 'stopTime', 'fromStationName', 'fromStationID', 'toStationName', 'toStationID', 'startLat', 'startLong', 'endLat', 'endLong', 'userType']
    
    def selected_columns(self)->list:
        return ['startTime', 'stopTime', 'bike', 'fromStationID', 'toStationID', 'userType']


class Station(AbstractSample):

    def __init__(self):
        super().__init__("stations/")

    def rename_columns_as(self, column_count:int=0):
        pass
    
    def selected_columns(self)->list:
        return ['id','name','latitude','longitude']
    
    def extract_sample(self, path: str)-> pd.DataFrame:
        return pd.read_csv(path, header=0, usecols = self.selected_columns())    

