#!flask/bin/python
from flask import Flask
from flask import request
import sqlite3

app = Flask(__name__)

#!Création ou réutilisation de la bdd
conn = sqlite3.connect('rest.db')

#!Création de la table
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS data(id TEXT,timestamp TEXT,sensorType INTEGER,value INTEGER)""")
conn.commit()

messages = []
nbMessageInArray = 0

@app.route('/messages', methods=['POST'])
def save_messages():

	id = request.json["id"]
	timestamp = request.json["timestamp"]
	sensorType = request.json["sensorType"]
	value = request.json["value"]

	#!cursor.execute("INSERT INTO data VALUES (id, timestamp, sensorType, value)")
	#!conn.commit()
	return "OK"

@app.route('/synthesis', methods=['GET'])
def synthesis():
	return "OK"

if __name__ == '__main__':
    app.run(debug=True)
