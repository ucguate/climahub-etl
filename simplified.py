# ------------------------------------
# -------------- imports -------------
# ------------------------------------
import psycopg2 
from psycopg2 import Error
from pprint import pprint 
import requests
import json
import pandas as pd
import re 

# ------------------------------------
# ------------- functions ------------
# ------------------------------------
def insert_station(station_name, latitude, longitude, source_station_id, original_id):
  source_id = 4 # ETESA - HIDROMET PA
  query = """
    insert into station (source_id,station_name,latitude,longitude, source_station_id, original_id) 
      values ( '{}', '{}', '{}', '{}', '{}', '{}') on conflict do nothing;
    """.format(source_id, station_name, latitude, longitude, source_station_id, original_id)
  return query
  #cursor.execute(query)
  #pprint(cursor.fetchall())

def insert_data_all(variable_id, data_date, data_value, source_station_id):
  query = """
    INSERT INTO data_all(variable_id,data_date,data_value, station_id)
      VALUES({},'{}','{}',{}) on conflict do nothing;
    """.format(
        variable_id, data_date, data_value,
        "(SELECT id FROM station WHERE source_station_id = '{}')".format(source_station_id)
        )
  return query


# manipulate data functions
def get_estaciones(option = "JSON"):
  queryParams = {
      'option': option,
      'key': "098f6bcd4621d373cade4e832627b4f6",
  }
  response = requests.get("https://www.snet.gob.sv/dataservices/getEstaciones.php", params=queryParams)
  #print(response.json)
  data = json.loads(response.text)
  #data = data["sensores"]
  #pprint(data)

  #getting column names
  #variables = data["features"][1].keys()
  #print(variables)

  df = pd.DataFrame(data)
  df.rename(columns = {0:'id',1:'name',2:'lat',3:'lon',}, inplace = True)
  #display(df)
  return df;

# manipulate data functions
def get_data(option = "JSON"):
  queryParams = {
      'option': option,
      'key': "098f6bcd4621d373cade4e832627b4f6",
  }
  response = requests.get("https://www.snet.gob.sv/dataservices/getData.php", params=queryParams)
  #print(response.json)
  data = json.loads(response.text)
  #data = data["sensores"]
  #pprint(data)

  #getting column names
  #variables = data["features"][1].keys()
  #print(variables)

  df = pd.DataFrame(data)
  df.rename(columns = {0:'id_estacion',1:'param',2:'datetime',3:'valor',}, inplace = True)
  #display(df)
  return df;

# manipulate data functions
def get_parametros(option = "JSON"):
  queryParams = {
      'option': option,
      'key': "098f6bcd4621d373cade4e832627b4f6",
  }
  response = requests.get("https://www.snet.gob.sv/dataservices/getParametros.php", params=queryParams)
  #print(response.json)
  data = json.loads(response.text)
  #data = data["sensores"]
  #pprint(data)

  #getting column names
  #variables = data["features"][1].keys()
  #print(variables)

  df = pd.DataFrame(data)
  #df.rename(columns = {0:'id',1:'name',2:'lat',3:'lon',}, inplace = True)
  #display(df)
  return df;

# ------------------------------------
# ------------ connection ------------
# ------------------------------------  
connection = None
try:
    # Connect to an existing database
    connection = psycopg2.connect(user="climahub",
                                  password="climahub$2021",
                                  host="138.197.98.178",
                                  port="5432",
                                  database="climahub")

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("🟢 CONNECTION SUCCESFUL: PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, Error) as error:
    print("❌ Error while connecting to PostgreSQL", error)
  
# --------------------------------------------------------------------------
# -------------- get estaciones - data & insert ----------------------------
# --------------------------------------------------------------------------
dfES = get_estaciones();
dfES.set_index("id", inplace=True)
dfDA = get_data();
variables = {
  'PC': ["PCP", 5],
  'PP': ["PP", 17],
  'HG': ["LEV", 13],
  'AT': ["TMP", 3],
  'BP': ["PRS", 10], 
  'DA': ["WND", 7],
  #'DI': ["PCP", 5],
  #'DR': ["PCP", 5],
  'HR': ["HSOL", 14],
  #'LH': ["HRP", 15],
  #'LT': ["PCP", 5],
  'RA': ["WNG", 9],
  #'RD': ["PCP", 5],
  'RH': ["HRP", 15],
  'SA': ["WNS", 8],
  #'SI': ["PCP", 5],
  'SR': ["RSOL", 16],
  #'UH': ["PCP", 5],
  #'UT': ["PCP", 5],
  #'BA': ["PCP", 5],
  'DP': ["DWP", 4],
  #'QI': ["PCP", 5],
  #'PD': ["PCP", 5],
  #'RI': ["PCP", 5],
  #'SO': ["PCP", 5],
  #'MA': ["PCP", 5],
  #'SM': ["PCP", 5],
  #'LW': ["PCP", 5],
  # 'ST': ["PCP", 5],
}
GRAND_QUERY = ''
for index, row in dfES.iterrows():
    #print(index,' - ',row['name'], '(', row['lat'],', ',row['lon'], ')' )
    GRAND_QUERY+=insert_station(row['name'], row['lat'], row['lon'], index, index)

#print(GRAND_QUERY)
cursor.execute(GRAND_QUERY)
connection.commit()

GRAND_QUERY = ''
for index, row in dfDA.iterrows():
  if(row['param'] != "  " and row['param'] in variables):
    #print(index,' - ',row['id_estacion'],' - ',row['param'],' - ',row['datetime'],' - ',row['valor'])
    GRAND_QUERY+=insert_data_all(variables[row['param']][1],row['datetime'], row['valor'],row['id_estacion'] )
    
#print(GRAND_QUERY)
cursor.execute(GRAND_QUERY)
connection.commit()

cursor.close()
connection.close()
print("PostgreSQL connection is closed")
