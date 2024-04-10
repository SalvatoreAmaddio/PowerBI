import pandas as pd

def remove_last_col(sample:pd.DataFrame)->pd.DataFrame:
    return sample.drop(sample.columns[-1], axis=1)

def print_to_csv(sample:pd.DataFrame, file_name:str):
    sample.to_csv(file_name, index=False)