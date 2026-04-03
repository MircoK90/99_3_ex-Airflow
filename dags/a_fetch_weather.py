# 1_fetch_weather

# from airflow.models import Variable
import requests
import os
import json
from datetime import datetime


# Task1

RAW_DIR   = os.getenv("raw_files",   "/app/raw_files")
CLEAN_DIR = os.getenv("clean_data", "/app/clean_data")


# Helper API key loader
def load_api_key():
    # 1) Versuchen, Airflow zu importieren
    try:
        from airflow.models import Variable
        # 2) Versuchen, Variable zu laden
        return Variable.get("API_KEY")
    except Exception:
        # 3) Fallback: .env laden
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv("API_KEY")


def fetch_weather(**kwargs):

    API_KEY = load_api_key()
    if not API_KEY:
        raise RuntimeError("API_KEY konnte nicht geladen werden!")
    
    # cities = Variable.get("cities", deserialize_json=True)
    # api_key = Variable.get("api_key")

    cities = ["Paris", "London", "Tokyo"]  # Example city list

    results = []

    for city in cities:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?q={city}&appid={API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        # important to raise an exception if the request failed, so that the task is marked as failed in Airflow
        resp.raise_for_status()
        data = resp.json()

        # 
        results.append({
            "name": data["name"],  # official city name from API
            "main": {
                "temp" : data["main"]["temp"],                                  # double main from api response ok!
                "pressure" : data["main"]["pressure"]
            }
        })

    # Filename = timestamp of collection, same pattern as the exercise
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    filepath  = os.path.join(RAW_DIR, f"{timestamp}.json")

    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved {len(results)} cities → {filepath}")




if __name__ == "__main__":
    RAW_DIR = "./raw_files"
    fetch_weather()




# mk info api resp

# curl -X GET "https://api.openweathermap.org/data/2.5/weather?q=paris&appid=16bbf99e4eacf331421675e1e52a32d1"

# {"coord":{"lon":2.3488,"lat":48.8534},„
# weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],
# "base":"stations","main":{"temp":281.15,"feels_like":279.55,„
# temp_min":279.94,"temp_max":281.58,
# "pressure":1018,"humidity":94,"sea_level":1018,"grnd_level":1008},„
# visibility":10000,"wind":{"speed":2.57,"deg":280},
# "clouds":{"all":100},"dt":1775107713,"sys":{"type":2,"id":2012208,
# "country":"FR","sunrise":1775107588,
# "sunset":1775154099},
# timezone":7200,
# "id":2988507,"name":
# "Paris","cod":200

