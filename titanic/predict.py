import pandas as pd
from utils import load_model
from preprocess import preprocess_data
from config import MODEL_PATH

def predict(input_csv):
    model = load_model(MODEL_PATH)
    df = pd.read_csv(input_csv)
    df = preprocess_data(df)
    preds = model.predict(df)
    return preds
