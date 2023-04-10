import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os


# aceasta functie apeleaza un link si returneaza rezultatul sub forma de .json
#def api_reader(link):
#    try:
#        logging.info(f"Requesting data from API at {link}")
#        call_api = requests.get(link,verify=False)
#        logging.info(f"Response status code: {call_api.status_code}")
#        call_api.raise_for_status()
#        data = call_api.json()
#        logging.info(f"Successfully retrieved data: {data}")
#        return data
#    except Exception as e:
#        logging.error(f"Error occurred while retrieving data from API: {str(e)}")
#        return None


def data_reader(link, dictionary):
    try:
        logging.info(f"Requesting data from API at {link}")
        call_api = requests.get(link,verify=False)
        logging.info(f"Response status code: {call_api.status_code}")
        call_api.raise_for_status()
        data = call_api.json()
        logging.info(f"Successfully retrieved data: {data}")
        response = pd.DataFrame(data[dictionary])
        return response
    except Exception as e:
        logging.error(f"Error occurred while retrieving data from API: {str(e)}")
        return None
    

#aceasta functie apeleaza o baza de date
def last_data(query,endpoint,port,name,user,password):
    engine = create_engine(f'postgresql://{user}:{password}@{endpoint}:{port}/{name}').connect().execute(query) 
    ####engine = db_connection(f'postgresql://{user}:{password}@{endpoint}:{port}/{name}').connect().execute(query)
    result = engine.first()
    #daca baza de date este goala, result = None. Cazul trebuie tratat pentru verificarea datelor introduse anterior
    if result is not None:
        last = result[0]
        return last
    return None

def file_exist(file,data):
    filename = f"{file}{data}.csv"
    time = datetime.strptime(data.strftime("%Y-%m-%d"), "%Y-%m-%d")
    # get the year and month from the date
    year = time.strftime("%Y")
    month = time.strftime("%m")
    # create the directories if they don't exist
    os.makedirs(os.path.join(year, month), exist_ok=True)

    # create the file path
    filepath = os.path.join(year, month, filename)
    if not os.path.exists(filepath):
        message = f'Fisierul {file}{data} nu exista la calea {filepath}. Incercam sa obtinem datele'
        return message
    else: 
        print(f'Fisierul {file}{data} exista la calea {filepath}. Se trece la data urmatoare')
        return None

def write_to_file(response,file,data):
    filename = f"{file}{data}.csv"
    time = datetime.strptime(data.strftime("%Y-%m-%d"), "%Y-%m-%d")
    # get the year and month from the date
    year = time.strftime("%Y")
    month = time.strftime("%m")
    # create the directories if they don't exist
    # create the file path
    filepath = os.path.join(year, month, filename)
    open(filepath, "w")
    #with open(filename, "w") as f:
    #    f.write("ticker,volume,vwap,open,close,high,low,timestamp,num_securities,average,date\n")
    response.to_csv(filepath, index=False)
    print(f'Informatia din data de {data} a fost procesata si adaugata in fisierul grouped_daily_bars_{data}.csv')
