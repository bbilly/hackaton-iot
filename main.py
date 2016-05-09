#!flask/bin/python
# coding: utf-8
#Seulement pour debug
from __future__ import print_function

from flask import Flask, request, jsonify
from celery import Celery

import sys
import sqlite3
import settings

app = Flask(__name__)

#Récupération des informations de configuration
app.config.from_object(settings)
#Création ou réutilisation de la bdd
conn_uri = 'rest.db'
#Connection à la base de données
conn = sqlite3.connect(conn_uri)
#Création du curseur
cursor = conn.cursor()
#Désynchronisation pour gagner en performance, perte de stabilité en contrepartie.
cursor.execute("""PRAGMA synchronous=OFF""")
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


@celery.task(name="main.add")
def add(data):

    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    cursor.execute("""BEGIN TRANSACTION""")
    for data in dataList:
        cursor.execute("""INSERT INTO data(id,timestamp,sensorType,value) VALUES (:id,:timestamp,:sensorType,:value)""", data)
    conn.commit()


@celery.task(name="main.get")
def get(end_timestamp, duration):
    #On commit la liste en base avant de faire le get afin de s'assurer de la concordande des données
    add(dataList)
    del dataList[:]
    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    if(duration == 0):
        cursor.execute('SELECT * FROM data WHERE timestamp BETWEEN ? AND ?', (end_timestamp, end_timestamp))
        rows = cursor.fetchall()
        for row in rows:
            print (row[0], file=sys.stderr)
    else:
        cursor.execute('SELECT * FROM data WHERE timestamp BETWEEN ? AND ?', (end_timestamp, end_timestamp))
        rows = cursor.fetchall()
        for row in rows:
            print (row[0], file=sys.stderr)

    #test
    data = {
        "sensorType": 1,
        "minValue": 1,
        "maxValue": 42,
        "mediumValue": 27.42
    }

    return jsonify(synthesis=[data, data, data, data, data, data, data, data, data, data])


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
    if len(dataList) == 1000:
        add(dataList)
        del dataList[:]
    return "Ok"


#Service REST fournissant une synthèse des données sur les x secondes passées en paramètre à partir du timestamp fourni
@app.route('/messages/synthesis', methods=['GET'])
def synthesis():

    timestamp = request.args.get('timestamp')
    duration = request.args.get('duration')
    ret = get(timestamp, duration)
    return ret

if __name__ == '__main__':
    app.run(debug=True)
