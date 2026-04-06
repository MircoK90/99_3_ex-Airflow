# e_train.py
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEAN_DIR = os.path.join(BASE, "clean_data")
MODEL_DIR = os.getenv("model_dir", os.path.join(BASE, "models"))



def train_and_save_model(model, X, y, path_to_model):
    # Train Models, compatible to LogisticRegression, DecisionTreeRegressor, RandomForestRegressor ...
    os.makedirs(os.path.dirname(path_to_model), exist_ok=True)
    model = model
    model.fit(X, y)
    dump(model, path_to_model)
    print(f"{str(model)} saved to {path_to_model}")


def evaluate_model(model, X, y):
    # Evaluate the model using cross-validation
    cross_validation = cross_val_score(model, X, y, cv=3, scoring='neg_mean_squared_error')
    return cross_validation.mean()



if __name__ == '__main__':
    from d_prepare_data import prepare_data  # prepare X,y from d_prepare_data.py

    X, y = prepare_data(os.path.join(CLEAN_DIR, 'data_test.csv'))

    if X.empty:
        print("ERROR: empty dataset - check data pipeline")
        exit(1)

    score_lr = evaluate_model(LinearRegression(), X, y)
    score_dt = evaluate_model(DecisionTreeRegressor(), X, y)
    score_rf = evaluate_model(RandomForestRegressor(), X, y)

    print(f"LinearRegression score:    {score_lr:.4f}")
    print(f"DecisionTreeRegressor score: {score_dt:.4f}")
    print(f"RandomForestRegressor score: {score_rf:.4f}")

    # lower neg_MSE (closer to 0) wins


    scores = {
        "LinearRegression":     (score_lr, LinearRegression()),
        "DecisionTreeRegressor":(score_dt, DecisionTreeRegressor()),
        "RandomForestRegressor":(score_rf, RandomForestRegressor()),
    }

    # eval which performed best and save it
    best_name, (best_score, best_model) = max(scores.items(), key=lambda x: x[1][0])
    print(f"Best model: {best_name} with score {best_score:.4f}")
    train_and_save_model(best_model, X, y, os.path.join(MODEL_DIR, 'best_model.joblib'))