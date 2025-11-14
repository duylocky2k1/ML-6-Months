import joblib
import pandas as pd

def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def save_model(model, path: str):
    joblib.dump(model, path)

def load_model(path: str):
    return joblib.load(path)
