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

# Init app
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
# Database           
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class User(db.Model): 
	
	username = db.Column(db.Text, unique=True, nullable=False, primary_key=True)
	password = db.Column(db.Text,  nullable=False)
	
db.create_all()

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

@app.route('/api/v1/users', methods=['PUT'])
def add_user():

	with counter.get_lock():
	    counter.value += 1

	if request.method != 'PUT':
		return jsonify({}),405

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

	r = requests.post(url=" http://127.0.0.1/api/v1/db/read", json=user)

	resp = r.json()

	if(resp == "Yes"):
		#User Exist
		return jsonify({}),400
	else:
		w = requests.post(url=" http://127.0.0.1/api/v1/db/write", json=user)
		if(w):
			return jsonify({}),201
			# return jsonify("User Added"),201
		else:
			#Server Error
			return jsonify({}),500

#2. Remove User
@app.route('/api/v1/users/<uname>', methods=['DELETE'])
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

	r = requests.post(url=" http://127.0.0.1/api/v1/db/read", json=R_user)
	
	a = r.json()
	if(a == "Yes"):

		w = requests.post(url=" http://127.0.0.1/api/v1/db/write", json=R_user)
		if(w):
			#User Removed
			return jsonify({}),200
		else:
			#Server Error
			return jsonify({}),500
	else:
		#User does Not Exist
		return jsonify({}),400

#8.Write to db

@app.route('/api/v1/db/write', methods=['POST'])
def DB_Write():

	if request.method != 'POST':
		return jsonify({}),405

	data=request.get_json()
	insert=data["insert"]
	column=data["column"]
	table=data["table"]
	#updated from here
	if(table=='User' and column=='deleteall'):
		r=User.query.all()
		print(r)
		db.session.delete(r)
		db.session.commit()
		return jsonify("all clear")
	if(table=='Ride' and column=="deleteall"):
		r=Ride.query.all()
		db.session.delete(r)
		db.session.commit()
		return jsonify("all clear")
	if(table=='JoinRide' and column=="deleteall"):
		r=JoinRide.query.all()
		db.session.delete(r)
		db.session.commit()
		return jsonify("all clear")
	#end of updation

	#Remove User from User table (usename is given)	
	if(table == "User" and column == "delete"):
		r = User.query.get(insert)
		db.session.delete(r)
		db.session.commit()
		return jsonify("del")
	
	#Add new User to User table 	
	if(table=="User"):
		newuser=User(username=insert[0], password=insert[1])
		try:
			db.session.add(newuser)
			db.session.commit()
		except:
			return jsonify({}),405
		return jsonify("Done"),201

	#Add new Ride to Ride table 	
	elif table=="Ride":
		now = strftime("%d-%m-%y:%S-%M-%H")
		now = "{}".format(now)
		new_ride = Ride(timestamp=dat_str_dattime(insert[1]), username=insert[0], source=insert[2] ,destination=insert[3])
		try:
			db.session.add(new_ride)
			db.session.commit()		
		except:
			return jsonify({}),405		
		return jsonify("Done"),201

	#Join existing ride / Add existing ride by adding new row to the JoinRide column 	
	elif table == "JoinRide":
		u = "{}".format(insert[1])
		try:
			join=JoinRide(rideId=insert[0], username=u)
			db.session.add(join)
			db.session.commit()
		except:
			return jsonify({}),405
		return jsonify("Done"),201

#9. Read from db

@app.route('/api/v1/db/read', methods=['POST'])
def DB_Read():

	if request.method != 'POST':
		return jsonify({}),405

	data = request.get_json()
	table = data["table"]
	column = data["column"]
	insert = data["insert"]
		
	#Read username to before deleting from User Table
	if(table == "User" and column == "delete"):	
		r = User.query.get(insert)
		if r:
			return jsonify("Yes")
		else:
			return jsonify("No")

	#read all the users  
	#update from here

	elif(table=="User" and column=="get_users"):
		result=[]
		res=User.query.all()
		for r in res:
			result.append(r.username)
		return json.dumps(result)
	#end of updation

	#Read username from User or Ride table befor inserting new User or Ride
	elif(table == "User"):
		name = User.query.get(insert[0])
		if name:
			return jsonify("Yes")
		else:
			return jsonify("No")
	
@app.route('/api/v1/users', methods=['GET'])
def user_list():

	with counter.get_lock():
	    counter.value += 1

	if request.method != 'GET':
		return jsonify({}),405

	input_read = {
		"table" : "User",
		"insert" : "",
		"column" : "get_users",
		} 
	response = requests.post(url = "http://127.0.0.1/api/v1/db/read",json=input_read)
	r = response.json()
	if(len(r) == 0):
		return jsonify({}),204
	return jsonify(response.json()),200

@app.route('/api/v1/db/clear',methods=['POST'])
def clear_database():
	if request.method != 'POST':
		return jsonify({}),405
	meta=db.metadata
	for table in reversed(meta.sorted_tables):
		db.session.execute(table.delete())
	db.session.commit()
	return jsonify({}),200

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

	if request.method != 'DELETE':
		return jsonify({}),405

	with counter.get_lock():
	    counter.value = 0

	return jsonify({}),200	

# Run Server
if __name__ == '__main__':
	app.run(host = "0.0.0.0",  port = "80",  debug=True)
