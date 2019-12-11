import mysql.connector #libreria para conectar MySQL con python
import os #Libreria del sistema operativo

#Se busca el directorio donde este alojado los CSV para luego importarlos
path = os.path.realpath(__file__)
directorio = os.path.dirname(path).replace('\\', '/')
#Se hace la conexion con la BBDD
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="recomendador"
)
mycursor = mydb.cursor()
#Se hace la sentencia del fichero CSV tags
sql = "LOAD DATA INFILE '"+directorio+"/tags.csv' INTO TABLE tags FIELDS TERMINATED BY ','; "
#Se ejecuta la sentencia
mycursor.execute(sql)
#Se hace la sentencia del fichero CSV ratings
sql = "LOAD DATA INFILE '"+directorio+"/ratings.csv' INTO TABLE ratings FIELDS TERMINATED BY ','; "
#Se ejecuta la sentencia
mycursor.execute(sql)
#Se hace la sentencia del fichero CSV links
sql = "LOAD DATA INFILE '"+directorio+"/links.csv' INTO TABLE links FIELDS TERMINATED BY ','; "
#Se ejecuta la sentencia
mycursor.execute(sql)
#Se hace la sentencia del fichero CSV movies
sql = "LOAD DATA INFILE '"+directorio+"/movies.csv' INTO TABLE movies FIELDS TERMINATED BY ','; "
#Se ejecuta la sentencia
mycursor.execute(sql)

mydb.commit()#Se envia a la base de datos
