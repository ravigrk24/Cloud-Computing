
from flask import Flask, request, jsonify, redirect, url_for, render_template
from flask_sqlalchemy import SQLAlchemy   
import requests 
import os 
import sys
import csv
import json
from time import strftime
from time import strptime
from datetime import datetime
from multiprocessing import Value



counter = Value('i', 0)
method = ["GET", "PUT", "POST", "HEAD", "DELETE"]
Dbaas = "3.94.83.112"


# Init app
app = Flask(__name__)

def read_csv():
	filename = "AreaNameEnum.csv"
	with open(filename,'r') as csvfile:
		csvreader = csv.reader(csvfile)
		next(csvreader)
		d={}
		for row in csvreader:
			en1 = int(row[0])
			dest = row[1]
			d[en1]=dest
	return d
				
def dat_str_dattime(sample):
	date,time = sample.split(':')
	dd,mm,yy = date.split('-')
	dd,mm,yy = int(yy),int(mm),int(dd)
	ss,m,hh = time.split('-')
	ss,m,hh = int(ss),int(m),int(hh)
	sample = (datetime(dd,mm,yy,hh,m,ss))
	return sample
	
#for environment variavle
@app.route("/")
def main():
	msg = os.getenv("TEAM_NAME", "no msg")
	return "<html><head><h1>" + msg + "<h1></head></html>"


#1. Add User

@app.route('/api/v1/users', methods=method)
def add_user():

	with counter.get_lock():
	    counter.value += 1

	if request.method == 'PUT':


		req= request.get_json()
		uname =req["username"]
		pword = req["password"]  

		if uname == "":
			return jsonify({}),400

		flag=True
		a=['A','B','C','D','E','F','a','b','c','d','e','f','0','1','2','3','4','5','6','7','8','9']
		if(len(pword)!=40 ):
			flag=False
		else:
			for i in pword:
				if(not(i in a)):
					flag=False

		if(flag==False):
			#Invalid Password
			return jsonify({}),400
		user={
			"insert":[uname, pword],
			"column":["username", "password"],
			"table" : "User"
		}

		r = requests.post(url="http://"+Dbaas+"/api/v1/db/read", json=user)
		print("hellllo")
		
		r = r.json()
		if(r == "Yes"):
			#User Exist
			return jsonify({}),400
		else:
			w = requests.post(url="http://"+Dbaas+"/api/v1/db/write", json=user)
			w = w.json()

			if(w == "Done"):
				return jsonify({}),201
				# return jsonify("User Added"),201
			else:
				#Server Error
				return jsonify({}),500

	elif(request.method == 'GET'):
		input_read = {
			"table" : "User",
			"insert" : "",
			"column" : "get_users",
			} 
		response = requests.post(url = "http://"+Dbaas+"/api/v1/db/read",json=input_read)
		r = response.json()
		r =json.loads(r)
		if(len(r) == 0):
			return jsonify({}),204
		return jsonify(r),200

	else:
		return jsonify({}),405
#2. Remove User
@app.route('/api/v1/users/<uname>', methods=method)
def removeUser(uname):	

	with counter.get_lock():
	    counter.value += 1

	if request.method != 'DELETE':
		return jsonify({}),405

	uname = "{}".format(uname)
	R_user = {
		"insert" : uname,
		"column" : "delete",
		"table" : "User"
	}

	r = requests.post(url="http://"+Dbaas+"/api/v1/db/read", json=R_user)
	
	a = r.json()
	if(a == "Yes"):
		w = requests.post(url="http://"+Dbaas+"/api/v1/db/write", json=R_user)
		w1 = w.json()
		if(w1 == "Del"):
			#User Removed
			return jsonify({}),200
		else:
			#Server Error
			return jsonify({}),500
	else:
		#User does Not Exist
		return jsonify({}),400



@app.route('/api/v1/db/clear',methods=['POST'])
def clear_database():
	if request.method != 'POST':
		return jsonify({}),405
	data={
		"insert":"",
		"column": "",
		"table" : "clear_db"
		}

	resp = requests.post(url="http://"+Dbaas+"/api/v1/db/write", json=data)
	r = resp.json()
	if(r == 'Done'):
		return jsonify({}),200
	return jsonify({}),500


#Get total HTTP requests made to microservice
@app.route('/api/v1/_count', methods=['GET'])
def HTTP_req_counter():
	if request.method != 'GET':
		return jsonify({}),405

	l = []
	l.append(counter.value)
	return jsonify(l),200

#Reset HTTP requests counter
@app.route('/api/v1/_count', methods=['DELETE'])
def HTTP_req_couter_reset():

	with counter.get_lock():
	    counter.value += 1

	if request.method != 'DELETE':
		return jsonify({}),405

	with counter.get_lock():
	    counter.value = 0

	return jsonify({}),200	

# Run Server
if __name__ == '__main__':
	app.run(host = "0.0.0.0",  port = "80",  debug=True)
