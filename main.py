#!flask/bin/python
# coding: utf-8
import collections
from flask import Flask, request
from celery import Celery
from dateutil.parser import parse
from dateutil.relativedelta import *
from datetime import datetime
from decimal import *
import os
import sqlite3
import settings
import urllib.parse
import json

app = Flask(__name__)

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
cursor.execute("""PRAGMA synchronous=WAL""")
#Création de la table
cursor.execute("""CREATE TABLE IF NOT EXISTS data(id TEXT,timestamp TEXT,sensorType INTEGER,value INTEGER)""")
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


def mean(listValues):
    if (listValues != []):
        result = Decimal(sum(listValues)) / len(listValues)
        return result.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


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
    return (str(ret) + "Z")


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
    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    dataSet = []

    #Création de la date de fin, le timestamp RFC3339 de fin est début_timestamp plus la durée
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

        data = collections.OrderedDict()
        data['sensorType'] = int(sensorType)
        data['minValue'] = int(minValue)
        data['maxValue'] = int(maxValue)
        data['mediumValue'] = float(meanValue)
        dataSet.append(data)

    return json.dumps(dataSet)


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
    app.run(host='0.0.0.0',port=80)
