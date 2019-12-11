import os
import mysql.connector
import pandas as pd
import numpy
import itertools
from multiprocessing import Process, Lock, Manager
import multiprocessing
import math
import sys
import json
import time


def pearson(lista,l , id2, umbral, listaID, listaP):
    #Se calcula el coeficiente Pearson de todos los usuarios respecto al usuario objetivo
    p = 0
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="recomendador"
    )

    mycursor = mydb.cursor()

    for par in lista:
        id1 = par


        u = pd.read_sql('SELECT ' + 'uno.user' + str(id1) + ',' + 'dos.user' + str(
            id2) + ' FROM( SELECT `userId`,`movieId`,'
                    '`rating`AS ' + 'user' + str(id1) + ' FROM `ratings` WHERE `userId`=' + str(
            id1) + ') AS uno INNER JOIN'
                    ' (SELECT `userId`,`movieId`,`rating` AS ' + 'user' + str(
            id2) + ' FROM `ratings` WHERE'
                    ' `userId`=' + str(id2) + ') AS dos ON uno.movieId = dos.movieId;', con=mydb)

        #Será necesario que el usuario tenga al menos 2 películas en común para calcular la similitud
        if not u.empty and len(u.index) > 2:
            u1 = "user"+str(id1)
            u2 = "user"+str(id2)
            mx = u.mean()[u1]
            my = u.mean()[u2]
            sum = 0

            for row in u.itertuples(index=True, name='Pandas'):
                x = row[1]
                y = row[2]


                if type(x) != float:
                    x = x.iloc[0]

                if type(y) != float:
                    y = y.iloc[0]

                sum = sum + ((x-mx) * (y-my))

            sumx = 0
            sumy = 0

            for row in u.itertuples(index=True, name='Pandas'):
                x = row[1]
                y = row[2]

                if type(x) != float:
                    x = x.iloc[0]

                if type(y) != float:
                    y = y.iloc[0]

                sumx = sumx + (x - mx)**2
                sumy = sumy + (y - mx)**2

                if type(sumx) != numpy.float64:
                    sumx = sumx.iloc[0]

                if type(sumy) != numpy.float64:
                    sumy = sumy.iloc[0]






            s = math.sqrt(sumx * sumy)

            if s == 0:
                sum = 0
                for row in u.itertuples(index=True, name='Pandas'):
                    x = row[1]
                    y = row[2]

                    if type(x) != float:
                        x = x.iloc[0]

                    if type(y) != float:
                        y = y.iloc[0]

                    dif = (x - y)**2
                    sum = sum + dif
                cor = -2 /(5 * len(u.index))
                pearson = cor * sum + 1


            else:
                pearson = (sum/s)


            if type(pearson) != float and type(pearson) != numpy.float64:
                pearson = pearson.iloc[0]

            if pearson > umbral:
                p = p + 1
                l.acquire()
                listaID.append(id1)
                listaP.append(pearson)
                l.release()

def ordenar(listapelis, numero):
    #Devuelve las n mejores películas ordenadas de mayor a menor
    listapelis = (sorted(listapelis, key=lambda x: x[-1], reverse=True))
    return listapelis[0:numero]


def comunes(chunk, l, id2, listaMas, listaID):
    #Guarda en ListaID la mejor combinacion de usuarios para hacer el dataframe comun
    #Estos usuarios seran los dos usuarios que más películas tengan en comun con el usuario objetivo (y entre sí)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="recomendador"
    )

    mycursor = mydb.cursor()

    query2 = 'SELECT `movieId`,`userId`,`rating` FROM `ratings` WHERE `userId`=' + id2 + ' or `userId`=' + str(listaID[0])

    for id in listaID:
        query2 += ' or `userId`=' + str(id)

    t = pd.read_sql(query2, con=mydb)
    t = t.pivot(index='userId', columns='movieId', values='rating')

    max = 0
    comun = pd.DataFrame()


    for index1 in chunk:
        if index1 != int(id2):
            for index2 in t.index:
                if index1 != index2:
                    if index2 != int(id2):
                        u1 = t[t.index == index1]
                        u2 = t[t.index == index2]
                        u3 = t[t.index == int(id2)]
                        dat = pd.concat([u1, u2, u3])
                        dat = dat.dropna(axis='columns')
                        if len(dat.columns) > max:
                            comun = dat
                            max = len(dat.columns)

    indices = comun.index.values.tolist()
    l.acquire()
    listaMas.append([tuple(indices), max])
    l.release()



#MAIN comenzamos obteniendo los argumentos de entrada
if __name__ == '__main__':

    if len(sys.argv) > 3:

        id2 = sys.argv[1]
        umbral = float(sys.argv[2])
        predicciones = int(sys.argv[3])
    else:
        if len(sys.argv) > 2:
            id2 = sys.argv[1]
            umbral = float(sys.argv[2])
            predicciones = 5
        else:
            if len(sys.argv) > 1:
                id2 = "0"
                umbral = 0
                predicciones = 5



    listaMejores = []

    #Estos objetos los utilizaremos para compartir recursos entre los hilos ejecutandose en paralelo
    with Manager() as manager:
        l = Lock()
        listaID = manager.list()
        listaP = manager.list()
        listaMas = manager.list()


        procs = []

        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database="recomendador"
        )

        mycursor = mydb.cursor()

        lista = []

        usuarios = pd.read_sql('SELECT `userId` FROM `ratings` WHERE 1 GROUP BY `userId` ', con=mydb)

        for id1 in usuarios.itertuples(index=True, name='Pandas'):
            if not id1[0] == int(id2):
                lista.append(id1[1])

        #Dividimos la lista en chunks en funcion del numero de nucleos logicos
        cpu = multiprocessing.cpu_count()
        length = len(lista)

        n = int(round(length/cpu))+1

        final = [lista[i * n:(i + 1) * n] for i in range((len(lista) + n - 1) // n)]


        #Se divide la tarea de calcular el coeficiente de Pearson en chunks
        #Estos chunks se calculan en paralelo en tantos hilos como de nucleos logicos se dispone
        for chunk in final:
            proc = Process(target=pearson, args=(chunk, l, id2, umbral, listaID, listaP))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()




        query2= 'SELECT `movieId`,`userId`,`rating` FROM `ratings` WHERE `userId`='+id2+' or `userId`='+str(listaID[0])
        query3='SELECT `movieId` FROM `ratings` WHERE `userId`='+id2
        for id in listaID:

            query2 +=' or `userId`='+str(id)
        # Con esta query obtenemos los posibles usuarios para hacer el dataframe comun
        t = pd.read_sql(query2, con=mydb)

        #Con esta query obtenemos las peliculas que ha visto nuestro usuario
        excluir = pd.read_sql(query3, con=mydb)

        t = t.pivot(index='userId', columns='movieId', values='rating')

        comun = pd.DataFrame()
        max = 0

        listacomun = list(t.index)
        n = int(round(len(listacomun) / cpu)) + 1

        final = [listacomun[i * n:(i + 1) * n] for i in range((len(listacomun) + n - 1) // n)]

        #Se divide la tarea de seleccionar los usuarios con más peliculas en comun con el objetivo
        for chunk in final:
            proc = Process(target=comunes, args=(chunk, l, id2, listaMas, listaID))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()

        #ordenamos la listapara obtener la mejor combinacion de usuarios
        output = sorted(listaMas, key=lambda x: x[-1], reverse=True)

        listamejor = list(output[0][0])


        #Creamos el dataframe con los usuarios con mas peliculas en comun con el objetivo
        sqlcomun = 'SELECT `movieId`,`userId`,`rating` FROM `ratings` WHERE `userId`='+id2
        for id in listamejor:
            sqlcomun +=' or `userId`='+str(id)

        comun = pd.read_sql(sqlcomun, con=mydb)
        comun = comun.pivot(index='userId', columns='movieId', values='rating')
        comun = comun.dropna(axis='columns')

        query = 'SELECT `movieId`,`userId`,`rating` FROM `ratings` WHERE `userId`='+id2

        for index1 in comun.index:
            query +=' or `userId`='+str(index1)


        #Se obtienen las películas que los usuarios del dataframe comun han valorado pero el usuario objetivo no
        target = pd.read_sql(query, con=mydb)
        #de las pelis que han valorado los usuarios del datafrmae, excluimos la que nuestro usuario objetivo tambien ha valorado
        target = target[~target.movieId.isin(excluir.movieId)]
        target = target.pivot(index='userId', columns='movieId', values='rating')

        target = target.dropna(axis='columns')



        sumaSim = 0

        for index in target.index:
            if str(index) != str(id2):
                sumaSim = sumaSim + listaP[listaID.index(index)]

        xid = comun.mean(axis=1).iloc[0]

        #Por cada pelicula en el dataframe target se concatena la columna correspondiente
        #y con el dataframe comun y sobre el resultado se calcula la prediccion
        for column in target.columns:
            dat1 = pd.concat([comun, target.loc[:, column]], axis=1)
            sumaM = 0
            for index in dat1.index:
                if str(index) != str(id2):
                    similitud = listaP[listaID.index(index)]
                    rating = dat1.loc[index][column]
                    media = dat1.mean(axis=1).loc[index]
                    sumaM = sumaM + (similitud * (rating - media))

            prediccion = xid + (sumaM / sumaSim)
            #creamos un tuple con la pelicula y su prediccion

            mycursor.execute("SELECT title FROM `movies` WHERE movieId="+str(column))
            titulos = mycursor.fetchall()
            listaMejores.append(tuple([column, titulos[0][0], round(prediccion, 2)]))

    #ordenamos la lista en orden descendente y la acortamos a los n mejores
    listaMejores = ordenar(listaMejores, predicciones)

    rs = json.dumps(listaMejores)
    print(rs)
