import psycopg2
from sqlalchemy import create_engine

#aceasta functie apeleaza un endpoint:port/name cu un user si parola si returneaza un engine
def db_connection(endpoint,port,name,user,password):
    try:
        connection = psycopg2.connect(host=endpoint, port=port, database=name,user=user,password=password)
        print("Database connection successful!")
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
    finally:
        connection.close()
    engine = create_engine(f'postgresql://{user}:{password}@{endpoint}:{port}/{name}')   
    return engine