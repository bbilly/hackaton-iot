#!flask/bin/python
# coding: utf-8
<<<<<<< HEAD
import collections
from flask import Flask, request
=======
#Seulement pour debug
from __future__ import print_function
import collections
from flask import Flask, request, jsonify
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7
from celery import Celery
from dateutil.parser import parse
from dateutil.relativedelta import *
from datetime import datetime
<<<<<<< HEAD
from decimal import *
import os
import sqlite3
import settings
import urllib.parse
import json

app = Flask(__name__)
=======
import os
import sqlite3
import settings
import functools
import urllib.parse
import sys
import time
import json

app = Flask(__name__)
app.debug = True
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7

#Permet de garder l'ordre des dictionnaire lors de la jsonify du dictionnaire des données
app.config["JSON_SORT_KEYS"] = False
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
#Vidage de la base au démarrage
try:
    os.remove(settings.DATABASE)
except OSError:
    pass
#Récupération des informations de configuration
app.config.from_object(settings)
#Création ou réutilisation de la bdd
conn_uri = settings.DATABASE
#Connection à la base de données
conn = sqlite3.connect(conn_uri)
#Création du curseur
cursor = conn.cursor()
#Désynchronisation pour gagner en performance, perte de stabilité en contrepartie.
cursor.execute("""PRAGMA synchronous=OFF""")
#Création de la table
cursor.execute("""CREATE TABLE IF NOT EXISTS data(id INTEGER,timestamp TIMESTAMP,sensorType INTEGER,value INTEGER)""")
conn.commit()

#Création d'une liste pour stocker les données en mémoire pour éviter de commit les données trop souvent
dataList = []


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

celery = make_celery(app)


<<<<<<< HEAD
def mean(listValues):
    if (listValues != []):
        result = Decimal(sum(listValues)) / len(listValues)
        return result.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


=======
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7
#Cette fonction permet d'ajouter la durée au RF3339 de début et retourne
#une RFC3339 de fin au bont format.
def get_end_timestamp(duration, begin_timestamp):
    #Parsing de la date et ajout de la durée
    end_date = parse(begin_timestamp)+relativedelta(seconds=+int(duration))
    #Le format ne respectant pas parfaitement la RFC, on recupère que jusqu'au centaines sur les milisecondes.
    try:
        ret = datetime.strptime(str(end_date).split("+")[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    except:
        try:
            ret = datetime.strptime(str(end_date).split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        except:
            raise
<<<<<<< HEAD
    return (str(ret) + "Z")
=======
    return end_date.timestamp()
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7


@celery.task(name="main.add")
def add(dataList):

    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    cursor.execute("""BEGIN TRANSACTION""")
    cursor.executemany("""INSERT INTO data(id,timestamp,sensorType,value) VALUES (:id,:timestamp,:sensorType,:value)""", dataList)
    conn.commit()
    del dataList[:]


@celery.task(name="main.get")
def get(begin_timestamp, duration):
    #On commit la liste en base avant de faire le get afin de s'assurer de la concordande des données
    add(dataList)
<<<<<<< HEAD
=======
    time.sleep(1)
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7
    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    dataSet = []

    #Création de la date de fin, le timestamp RFC3339 de fin est début_timestamp plus la durée
<<<<<<< HEAD
    begin = str(begin_timestamp)
    end = get_end_timestamp(duration, begin_timestamp)
    cursor.execute("""SELECT DISTINCT sensorType FROM data WHERE timestamp BETWEEN ? AND ? ORDER BY sensorType""", (begin, end))
    sensorTypes = [row[0] for row in cursor.fetchall()]
    for sensorType in sensorTypes:

        cursor.execute("""SELECT value FROM data WHERE sensortype = ? AND timestamp BETWEEN ? AND ?""", (sensorType, begin, end))
        valuesBysensorType = [row[0] for row in cursor.fetchall()]
        minValue = min(int(minValue) for minValue in valuesBysensorType)
        maxValue = max(int(maxValue) for maxValue in valuesBysensorType)
        #meanValue = functools.reduce(lambda x, y: x + y, valuesBysensorType) / len(valuesBysensorType)
        meanValue = mean(valuesBysensorType)
=======
    begin = parse(begin_timestamp).timestamp()
    end = get_end_timestamp(duration, begin_timestamp)
    print("Begin : " + str(begin), file=sys.stderr)
    print("End : " + str(end), file=sys.stderr)
    cursor.execute("""SELECT sensorType FROM data GROUP BY sensorType""")
    sensorTypes = [row[0] for row in cursor.fetchall()]

    for sensorType in sensorTypes:
        a = int(sensorType)
        cursor.execute("SELECT value FROM data WHERE sensorType=?", (a,))
        valuesBysensorType = [row[0] for row in cursor.fetchall()]
        minValue = min(int(minValue) for minValue in valuesBysensorType)
        maxValue = max(int(maxValue) for maxValue in valuesBysensorType)
        meanValue = functools.reduce(lambda x, y: x + y, valuesBysensorType) / len(valuesBysensorType)
        meanValue = "%.2f" % meanValue
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7

        data = collections.OrderedDict()
        data['sensorType'] = int(sensorType)
        data['minValue'] = int(minValue)
        data['maxValue'] = int(maxValue)
        data['mediumValue'] = float(meanValue)
        dataSet.append(data)
<<<<<<< HEAD

    return json.dumps(dataSet)
=======
    
    return json.dumps(dataSet, separators=(',', ': '))
>>>>>>> 61efaaf8a7a72152166c113cbeac8c5b417cd3d7


#Service REST d'acquisition de messages provenant d'objets connectés
@app.route('/messages', methods=['POST'])
def save_messages():

    data = {
        "id": request.json["id"],
        "timestamp": request.json["timestamp"],
        "sensorType": request.json["sensorType"],
        "value": request.json["value"]
    }
    dataList.append(data.copy())
    if len(dataList) == settings.NB_MSG_TO_COMMIT:
        add(dataList)
    return "Ok"


#Service REST fournissant une synthèse des données sur les x secondes passées en paramètre à partir du timestamp fourni
@app.route('/messages/synthesis', methods=['GET'])
def synthesis():

    #On s'assure que l'encodage URL est bien transformé
    timestamp = urllib.parse.unquote(urllib.parse.unquote(request.args.get("timestamp")))
    duration = request.args.get("duration")
    return get(timestamp, duration)

if __name__ == '__main__':
    app.run()
