#!flask/bin/python
from flask import Flask,request
from celery import Celery
import sqlite3
import requests

import settings

app = Flask(__name__)

#Récupération des informations de configuration
app.config.from_object(settings)

#!Création ou réutilisation de la bdd
conn_uri = 'rest.db'

#!Création de la table
conn = sqlite3.connect(conn_uri)
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS data(id TEXT,timestamp TEXT,sensorType INTEGER,value INTEGER)""")
conn.commit()

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
    cursor.execute("""INSERT INTO data(id,timestamp,sensorType,value) VALUES (:id,:timestamp,:sensorType,:value)""",data)
    conn.commit()

def getSensorType():

    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    data = cursor.execute("""SELECT DISTINCT sensorType FROM data""")
    conn.commit()
    return data
@celery.task(name="main.get")
def get():

    conn = sqlite3.connect(conn_uri)
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO data(id,timestamp,sensorType,value) VALUES (:id,:timestamp,:sensorType,:value)""",data)
    conn.commit()
    return 


#Utilisation de Flask pour la gestion du serveur REST
@app.route('/messages', methods=['POST'])
def save_messages():

    data = { 
    "id" : request.json["id"],
    "timestamp" : request.json["timestamp"],
    "sensorType" : request.json["timestamp"],
    "value" : request.json["value"] }

    add(data)
    return "OK"

@app.route('/synthesis', methods=['GET'])
def synthesis():

    return "OK"

if __name__ == '__main__':
    app.run(debug=True)
