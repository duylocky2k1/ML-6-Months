from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from preprocess import preprocess_data
from utils import load_data, save_model
from config import DATA_PATH, MODEL_PATH, TARGET_COL

def train_model():
    df = load_data("https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv")
    df = preprocess_data(df)
    X = df.drop(columns=[TARGET_COL])
    y = df[TARGET_COL]

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression(max_iter=200)
    model.fit(X_train, y_train)

    preds = model.predict(X_val)
    acc = accuracy_score(y_val, preds)
    print(f"Validation accuracy: {acc:.4f}")

    save_model(model, MODEL_PATH)

if __name__ == "__main__":
    train_model()
