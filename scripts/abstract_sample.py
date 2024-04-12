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

    def month_improvement(self, months:pd.Series, addition:int)->int:
        """
        This function helps to extract a sample from the dataset.
        """
        if months.isin([1]).any():
            return 100 + addition
        if months.isin([2]).any():
            return 200 + addition
        if months.isin([3]).any():
            return 300 + addition
        if months.isin([4]).any():
            return 400 + addition
        if months.isin([5]).any():
            return 500 + addition
        if months.isin([6]).any():
            return 600 + addition
        if months.isin([7]).any():
            return 700 + addition
        if months.isin([8]).any():
            return 800 + addition
        if months.isin([9]).any():
            return 900 + addition
        if months.isin([10]).any():
            return 1000 + addition
        if months.isin([11]).any():
            return 1100 + addition
        if months.isin([12]).any():
            return 1300 + addition
        return 0
    
    def year_improvement(self, years:pd.Series, months:pd.Series)->int:
        """
        This function helps to extract a sample from the dataset
        """
        if years.isin([2014]).any():
            return self.month_improvement(months,100)
        if years.isin([2015]).any():
            return self.month_improvement(months,200)
        if years.isin([2016]).any():
            return self.month_improvement(months,300)
        if years.isin([2017]).any():
            return self.month_improvement(months,400)
        if years.isin([2018]).any():
            return self.month_improvement(months,500)
        if years.isin([2019]).any():
            return self.month_improvement(months,600)
        if years.isin([2020]).any():
            return self.month_improvement(months,250)
        if years.isin([2021]).any():
            return self.month_improvement(months,800)
        if years.isin([2022]).any():
            return self.month_improvement(months,900)
        return 0
    
    def extract_sample(self, path: str)-> pd.DataFrame:
        print(f"reading {path}")
        #read the headers
        df_header = pd.read_csv(path, nrows=0)
        #get column count
        column_count = len(df_header.columns)
        #uniform column names
        self.rename_columns_as(column_count)
        #read the csv again by providing uniform column naming and selecting only necessary columns
        data_set = pd.read_csv(path, names=self.new_column_names, header=0, usecols = self.selected_columns())    
        #convert startTime and stopTime to date format.
        data_set['startTime'] = pd.to_datetime(data_set['startTime'], errors='coerce')
        data_set['stopTime'] = pd.to_datetime(data_set['stopTime'], errors='coerce')
        #get the Month for grouping 
        month = data_set['startTime'].dt.month 
        data_set['month'] = data_set['startTime'].dt.month  
        #get the Year to simulate Business Trend.
        year = data_set['startTime'].dt.year
        #ensure the stationsIDs are treated as strings.
        data_set['fromStationID'] = data_set['fromStationID'].astype(str)
        data_set['toStationID'] = data_set['toStationID'].astype(str)
        #ensure that sample drops nan values or not numeri values
        data_set = data_set[pd.notna(data_set['fromStationID']) & data_set['fromStationID'].str.isnumeric()]
        data_set = data_set[pd.notna(data_set['toStationID']) & data_set['toStationID'].str.isnumeric()]
        #return the sample groupped by month. Select only some rows by calling year_improvement.
        return data_set.groupby('month').head(self.year_improvement(year, month))

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
        print(f"reading {path}")
        return pd.read_csv(path, header=0, usecols = self.selected_columns())    

