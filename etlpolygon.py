# import functii
from adapter import data_reader, write_to_file

# import variable
from variables import polygon_url, polygon_api_key


#import modules


def polygon(data, engine):
    # Apeleaza functia care cheama un API si returneaza un tuple ce contine datele expuse de acel API
    # Functia se apeleaza cu link catre API ai dictionarul al carui continut sa il returneze
    response = data_reader(f'{polygon_url}{data}?apiKey={polygon_api_key}', 'results')
    # Daca data_reader returneaza None, pentru ca procesul sa nu se opreasca, sarim intentionat la urmatorul proces
    if response is None:
        print(f'In data de {data} nu sunt date. Probabil piata a fost inchisa. Se trece la ziua urmatoare')
        pass
    else:
        # Stim ca atunci cand today este zi de weelend, raspunsul API-ului nu contine 'results'
        # Dar chiar daca am tratat acest caz, pentru a economisii 2 calluri din cele 5/minut
        # tratam aceasta situatie si sarim peste zilele de weekend
        if data.weekday() <= 5:
            ######################################
            ###### Se proceseaza informatia ######
            ######################################
            response.rename(columns = {'T':'ticker','v':'volume','vw':'vwap','o':'open','c':'close','h':'high','l':'low','t':'timestamp','n':'num_securities'}, inplace = True)
            response['average'] = (response['high']+response['high']+response['close'])/3
            response['date'] = data

            response.to_csv('grouped_daily_bars.csv', index=False)

            print(f'Informatia din data de {data} a fost procesata si adaugata in baza de date')
            #print(response)

            
            #response.to_sql('grouped_daily_bars', engine, if_exists='append', index=False)

        else:
            print(f'In data de {data} piata a fost inchisa. Se trece la ziua urmatoare')

def polygon_csv(data,file):
    # Apeleaza functia care cheama un API si returneaza un tuple ce contine datele expuse de acel API
    # Functia se apeleaza cu link catre API ai dictionarul al carui continut sa il returneze
    response = data_reader(f'{polygon_url}{data}?apiKey={polygon_api_key}', 'results')
    # Daca data_reader returneaza None, pentru ca procesul sa nu se opreasca, sarim intentionat la urmatorul proces
    if response is None:
        print(f'In data de {data} nu sunt date. Probabil piata a fost inchisa. Se trece la ziua urmatoare')
        pass
    else:
        # Stim ca atunci cand today este zi de weelend, raspunsul API-ului nu contine 'results'
        # Dar chiar daca am tratat acest caz, pentru a economisii 2 calluri din cele 5/minut
        # tratam aceasta situatie si sarim peste zilele de weekend
        if data.weekday() <= 5:
            ######################################
            ###### Se proceseaza informatia ######
            ######################################
            response.rename(columns = {'T':'ticker','v':'volume','vw':'vwap','o':'open','c':'close','h':'high','l':'low','t':'timestamp','n':'num_securities'}, inplace = True)
            response['date'] = data
            write_to_file(response, file, data)
        else:
            print(f'In data de {data} piata a fost inchisa. Se trece la ziua urmatoare')