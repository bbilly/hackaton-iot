#!flask/bin/python
from flask import Flask
from flask import request

app = Flask(__name__)

@app.route('/messages', methods=['POST'])
def save_messages():
	id = request.json["id"]
	timestamp = request.json["timestamp"]
	sensorType = request.json["sensorType"]
	value = request.json["value"]

	print "json : "+timestamp
	return "OK"

if __name__ == '__main__':
    app.run(debug=True)