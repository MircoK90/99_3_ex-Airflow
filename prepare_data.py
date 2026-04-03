
import pandas as pd
import os


# use relative path as fallback when running locally outside Docker
CLEAN_DIR = os.getenv("clean_data", os.path.join(os.path.dirname(__file__), "clean_data"))
RAW_DIR   = os.getenv("raw_files",  os.path.join(os.path.dirname(__file__), "raw_files"))


def prepare_data(path=os.path.join(CLEAN_DIR, 'data.csv')):
    
    """
    Data will be sorted for City and Date, down under is newest
    tmp for every City
    Target: temp from yesterday shift(1)
    feats: temp for the next 9 days (shift(-i)
    NANs from both sides gest deleted
    concantening in one dg again
    City get OneHotEncoded)
    """

    dfc = []                                    #dfcity
    df = pd.read_csv(path)

    # rename temperature column to temp for consistency
    df = df.rename(columns={"temperature": "temp"})
    df = df.sort_values(['city', 'date'], ascending=[True, True])               # two times True!

    for city in df["city"].unique():
        tmp = df[df["city"] == city].copy()
        tmp["target"] = tmp["temp"].shift(1)

        # last 9 Feats as lags
        for i in range(1,9):
            tmp[f"temp_lag_{i}"] = tmp["temp"].shift(-i)

        tmp = tmp.dropna()
        dfc.append(tmp)

    df_final = pd.concat(dfc, ignore_index=True)
    df_final = df_final.drop(columns=["date"])
    df_final = pd.get_dummies(df_final, columns=["city"], drop_first=True)
    
    X = df_final.drop(columns=["target"])
    y = df_final["target"]
    return X, y



if __name__ == "__main__":
    X, y = prepare_data(
        path=os.path.join(CLEAN_DIR, 'data_test.csv')
    )
    print(X.head())
    print(y.head())
