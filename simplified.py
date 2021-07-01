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
  source_id = 2 # ETESA - HIDROMET PA
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
def convert_date(date):
  #print(date)
  if(date[1:2] == '/'):
    date='0'+date
  if(date[17:19] == "AM"):
    #print("AM")
    time = date[11:16]+":00"
  else:
    #print("PM")
    hour = int(date[11:13])
    if(hour == 12):
      time = str(hour)+date[13:16]+":00"
    else:
      time = str(hour+12)+date[13:16]+":00"

  return date[6:10]+'-'+date[3:5]+'-'+date[0:2]+' '+time

def clean_valor(valor):
  valor = re.sub(r"watt/m2| |/|m/s|ºC|mbar|msnm|m|%|°", '', valor)
  return valor
# manipulate data functions
def get_metar(zoom = 18, filter = 'prior', density = 0, taf = 'false', bbox = '-104.94370625,1.7950665583439,-60.998393750001,28.156120443236'):
  queryParams = {
      'zoom': zoom,
      'filter': filter,
      'density': density,
      'taf': taf,
      'bbox': bbox
  }
  response = requests.get("https://927d1d30.us-south.apigw.appdomain.cloud/hidromet", params=queryParams)
  #print(response.json)
  data = json.loads(response.text)
  data = data["sensores"]
  #pprint(data)

  #getting column names
  #variables = data["features"][1].keys()
  #print(variables)

  df = pd.DataFrame(data)
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
  
# ------------------------------------
# -------------- metar ---------------
# ------------------------------------
dfPA = get_metar();
dfPA.set_index("codigo", inplace=True)
variables = {
  "DIR_VIENTO" : ["WND", 7],
  "HORA_SOL" : ["HSOL", 14],
  "HR_PROM" : ["HRP", 15],
  "LLUVIA" : ["PCP	", 5],
  "NIVEL" : ["LEV", 13],
  "P_BAROM" : ["PRS", 10],
  "RAD_SOLAR" : ["RSOL", 16],
  "RAFAGA" : ["WNG", 9],
  "TEMP_PROM" : ["TMP", 3],
  "VEL_VIENTO" : ["WNS", 8]
}
# iterating recognized variables to look for into df
GRAND_QUERY = ''
for key, value in variables.items():
  #print(key,value[0], value[1])
  cur_row = dfPA.loc[key].to_dict()
  #print('Iterando:', cur_row["nombre"], '\n')
  
  #iterating across "estaciones"
  for k, v, in cur_row["estaciones"].items():
    #print(k,v)
    GRAND_QUERY+=insert_station(v["nombre"], v["latitud"], v["longitud"], v["numero_estacion"], k)
    GRAND_QUERY+=insert_data_all(value[1], convert_date(v["sensor_fecha"]), clean_valor(v["sensor_valor"]), v["numero_estacion"])
    
  cursor.execute(GRAND_QUERY)
  connection.commit()

cursor.close()
connection.close()
print("PostgreSQL connection is closed")
