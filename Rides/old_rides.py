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

ride_ip = {"Origin" : "52.6.195.148"}
ld_ip = "RideShare-1436489136.us-east-1.elb.amazonaws.com"

# Init app
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

# Database           
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Ride(db.Model):

	rideId = db.Column(db.Integer, unique=True, nullable=False, primary_key=True,autoincrement=True)
	username = db.Column(db.Text, nullable=False)
	timestamp = db.Column(db.DateTime, nullable=False)
	source = db.Column(db.Integer, nullable=False)
	destination = db.Column(db.Integer, nullable=False)		


class JoinRide(db.Model):

	rideId = db.Column(db.Integer, nullable=False)
	username = db.Column(db.Text, nullable=False, primary_key=True)

db.create_all()

def dat_str_dattime(sample):
	date,time = sample.split(':')
	dd,mm,yy = date.split('-')
	dd,mm,yy = int(yy),int(mm),int(dd)
	ss,m,hh = time.split('-')
	ss,m,hh = int(ss),int(m),int(hh)
	sample = (datetime(dd,mm,yy,hh,m,ss))
	return sample

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

#for environment variavle
@app.route("/")
def main():
	msg = os.getenv("TEAM_NAME", "no msg")
	return "<html><head><h1>" + msg + "<h1></head></html>"


#3. Create ride
@app.route('/api/v1/rides', methods=['POST'])
def add_ride():

	with counter.get_lock():
		counter.value += 1

	if request.method != 'POST':
		return jsonify({}),405

	req = request.get_json()
	created_by = req["created_by"]
	timestamp = req["timestamp"]
	source = req["source"]
	destination = req["destination"]

	try:
		source = int(source)
		destination = int(destination)
		timestamp1 = datetime.strptime(timestamp,"%d-%m-%Y:%S-%M-%H")
	except Exception as e:
		
		return jsonify({}),400
		#return "Invalid Input",400	
	
	d=read_csv()
	if(source not in d.keys() or destination not in d.keys() or source == destination):
		return jsonify({}),400
		#return "Invalid area",400

	ride = {
		"insert":[created_by, timestamp, source, destination],
		"column":["username", "timestamp", "source", "destination"],
		"table" : "Ride"
	}
	now = datetime.now()
	date = dat_str_dattime(timestamp)
	
	if(date<now):
		return jsonify({}),400
		# return "invalid timestamp",400

	user = {
		"insert":[created_by],
		"column":"",
		"table" : "User"
		}
		#check username in User table

	u = requests.get(url="http://"+ld_ip+"/api/v1/users", headers = ride_ip)
	
	b = u.json()
	if created_by in b:
		w = requests.post(url=" http://127.0.0.1/api/v1/db/write", json=ride)
		if(w):
			
			return jsonify({}),201
		else:
			#server Error
			return jsonify({}),500
	else:
		#No Username
		return jsonify({}),400
			
	return jsonify({}),500
	#Server Error

#4.List all upcoming rides for a given source and destination

@app.route('/api/v1/rides',methods = ['GET'])
def view_rides():

	with counter.get_lock():
		counter.value += 1

	if request.method != 'GET':
		return jsonify({}),405
	
	s = request.args.get('source')
	d = request.args.get('destination')

	input_read = {
		"table" : "Ride",
		"insert" : "",
		"column" : "get_ride",
		"source" : s,
		"destination" : d
	} 
	response = requests.post(url = "http://127.0.0.1/api/v1/db/read",json=input_read)
	r = response.json()
	if(len(r) == 0):
		return jsonify({}),204
	return jsonify(response.json()),200

#5. List all the details of a given ride		

@app.route('/api/v1/rides/<r>',methods = ['GET'])
def view_users(r):

	with counter.get_lock():
		counter.value += 1

	if request.method != 'GET':
		return jsonify({}),405

	input_read = {
		"table" : "Ride",
		"insert" : r,
		"column" : "get_users",
		} 
	response = requests.post(url = "http://127.0.0.1/api/v1/db/read",json=input_read)
	r = response.json()
	if(len(r) == 0):
		return jsonify({}),204
	return jsonify(response.json()),200
	
#6. Join an existing ride

@app.route('/api/v1/rides/<rideID>', methods=['POST'])
def joinride(rideID):

	with counter.get_lock():
		counter.value += 1

	if request.method != 'POST':
		return jsonify({}),405

	req = request.get_json()
	uname = req["username"]

	J_ride = {
		"insert":[rideID, uname],
		"column":["rid", "username"],
		"table": "JoinRide"
	} 
	
	
	#check username in User table
	u = requests.get(url=" http://"+ld_ip+"/api/v1/users", headers = ride_ip)

	b = u.json()

	if uname in b:
		w = requests.post(url=" http://127.0.0.1/api/v1/db/write", json=J_ride)
		if(w):
			#Ride Joined
			return jsonify({}),200
		else:
			#Write Error
			return jsonify({}),500
	else:
		#No Username
		return jsonify({}),400

	return jsonify({}),500
	#Server Error

#7. Delete a ride

@app.route('/api/v1/rides/<rideId>', methods=["DELETE"])
def delete_ride(rideId):

	with counter.get_lock():
		counter.value += 1

	if request.method != 'DELETE':
		return jsonify({}),405

	D_ride = {
		"insert" : rideId,
		"column" : "delete",
		"table" : ["Ride", "JoinRide"]
	}
	
	r = requests.post(url=" http://127.0.0.1/api/v1/db/read", json=D_ride)
	a=r.json()

	if(a == "InRide" or a == "InJoin"):
		w = requests.post(url=" http://127.0.0.1/api/v1/db/write", json=D_ride)
		print(w.json())
		if(w):
			
			return jsonify({}),200
		else:
			#Server Error
			return jsonify({}),500
	else:

		#Ride Does Not Exist
		return jsonify({}),204

#8.Write to db

@app.route('/api/v1/db/write', methods=['POST'])
def DB_Write():

	if request.method != 'POST':
		return jsonify({}),405

	data=request.get_json()
	insert=data["insert"]
	column=data["column"]
	table=data["table"]

	#delete db
	if(table=='Ride' and column=="deleteall"):
		r=Ride.query.all()
		db.session.delete(r)
		db.session.commit()
		
		r=JoinRide.query.all()
		db.session.delete(r)
		db.session.commit()
		return jsonify("success")
	#end of updation

	#Delete Ride from Ride table (rideID is given)	
	if((table[0] == "Ride" or table[1] == "JoinRide") and column == "delete"):		
		r1 = Ride.query.get(insert)
		r2 = JoinRide.query.filter_by(rideId = insert).first()
		if r1:
			db.session.delete(r1)
		if r2:
			db.session.delete(r2)
		db.session.commit()
		return jsonify("del")
	
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
		
	#Counting total number of rides
	if insert == "total_rides":
		cnt = db.session.query(Ride).count()
		print(cnt)
		return jsonify(cnt)
		
	#Read rideID before delete from Ride Table
	if(table[0] == "Ride" or table[1] == "JoinRide" ) and (column == "delete"):	
		joinResponse = JoinRide.query.filter_by(rideId = insert).first() 
		RideResponse = Ride.query.get(insert)
		if RideResponse :
			return jsonify("InRide")
		elif joinResponse:
			return jsonify("InJoin")
		else:
			return jsonify("No")

	#Read from JoinRide table before adding new ride (given ride ID)
	elif(table == "JoinRide" ):
		ride_tab_u = JoinRide.query.get(insert[1])
		r = Ride.query.filter_by(username = insert[1]).first()
		if r:
			return jsonify("Yes")
		elif ride_tab_u:
			return jsonify("Yes")
		return jsonify("No")

	elif (table == "Ride" and column == "get_ride"):
		s = data["source"]
		d = data["destination"]
		row = Ride.query.filter(Ride.source == s,Ride.destination == d,Ride.timestamp > datetime.now()) 
		results = [] # list of dicionaries
		for row in row:
			result = {} 
			result["rideId"] = row.rideId
			result["username"] = row.username
			str_date = row.timestamp.strftime("%d-%m-%Y:%S-%M-%H")
			result["timestamp"] = str_date		
			results.append(result)
		return json.dumps(results)

	elif table == "Ride" and column =="get_users":
		r = data["insert"]
		res = Ride.query.filter_by(rideId = r)
		res1 = JoinRide.query.filter_by(rideId = r)
		users= []
		result = {}
		for row in res:
			result["rideId"] = row.rideId
			result["created_by"] = row.username
			str_date = row.timestamp.strftime("%d-%m-%Y:%S-%M-%H")
			result["timestamp"] = str_date
			for row1 in res1:
				users.append(row1.username)
			result["users"] = users
			result["source"] = row.source
			result["destination"] = row.destination
		return json.dumps(result)		


	elif table == "Ride":
		
		r = Ride.query.filter_by(username = insert[0]).first()
		
		if r: 
			return jsonify("Yes")
		else:
			return jsonify("No")
	
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

#Get total number of rides
@app.route('/api/v1/rides/count', methods=['GET'])
def total_rides():

	if request.method != 'GET':
		return jsonify({}),405

	Total_rides = {
		"insert":"total_rides",
		"column":"",
		"table": ["Ride","JoinRide"]
	}

	cnt = requests.post(url="http://127.0.0.1/api/v1/db/read", json=Total_rides)

	tot_rides = cnt.json()
	l = []
	l.append(tot_rides)
	return jsonify(l),200


# Run Server
if __name__ == '__main__':
	app.run(host="0.0.0.0", port="80", debug=True)

