# import functions
# MOVE FILE EXIST TO ADAPTER!!!!!!
from etlpolygon import polygon, polygon_csv
from adapter import file_exist
from conectors import db_connection

# import variables
from variables import db_endpoint, db_port, db_name, db_user, db_password

from datetime import date, timedelta
import time


def handler(event, response):
    today = date.today()
#    engine = db_connection(db_endpoint,db_port,db_name,db_user,db_password)
#    history = last_data('select distinct date from public.grouped_daily_bars order by date desc limit 1;',
#                        db_endpoint,
#                        db_port,
#                        db_name,
#                        db_user,
#                        db_password)
    # daca baza de date este goala, history = ziua curenta - 2 ani
    # daca baza de date contine elemente history = ultimul element + 1 zi 
#    if history is None:
#        history = today - timedelta(days=730)
#    elif history == today - timedelta(days=1):
#        print("Informatiile sunt la zi")
#    else:
#        print(f'In baza de date sunt informatii pana la data de {history}. Procesarea nolor date incepe de la {history+timedelta(days=1)}')
#        history += timedelta(days=1)
    file = 'grouped_daily_bars_'
    history = today - timedelta(days=730)
    while history <= today:
        exista = file_exist(file,history)
        if exista is not None:
            polygon_csv(history, file)
            time.sleep(13)
        else:
            pass
        history += timedelta(days=1)


handler({}, {})
