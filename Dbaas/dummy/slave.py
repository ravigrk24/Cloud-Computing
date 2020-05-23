
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
import pika
import json

counter = Value('i', 0)
method = ["GET", "PUT", "POST", "HEAD", "DELETE"]
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

class User(db.Model): 
	username = db.Column(db.Text, unique=True, nullable=False, primary_key=True)
	password = db.Column(db.Text,  nullable=False)

db.create_all()
def dat_str_dattime(sample):
	date,time = sample.split(':')
	dd,mm,yy = date.split('-')
	dd,mm,yy = int(yy),int(mm),int(dd)
	ss,m,hh = time.split('-')
	ss,m,hh = int(ss),int(m),int(hh)
	sample = (datetime(dd,mm,yy,hh,m,ss))
	return sample
#------------------------------------------------CONNECTING TO RABBITMQ----------------------------------------------------
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel1 = connection.channel()
channel2 = connection.channel()
channel3 = connection.channel()

channel1.queue_declare(queue='writeQ')
channel2.queue_declare(queue='readQ')
# channel3.queue_declare(queue='syncQ', durable=True)

#Sync
channel3.exchange_declare(exchange='logs', exchange_type='fanout')
result = channel3.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel3.queue_bind(exchange='logs', queue=queue_name)


#-----------------------------------------------DB_WRITE OPERATIONS-----------------------------------------------------------
def DB_Write(write_data):

	# data=request.get_json()
	data = json.loads(write_data)

	insert=data["insert"]
	column=data["column"]
	table=data["table"]
	#updated from here
	if(table=='User' and column=='deleteall'):
		r=User.query.all()
		print(r)
		db.session.delete(r)
		db.session.commit()
		return "all clear"

	elif(table=='Ride' and column=="deleteall"):
		r=Ride.query.all()
		db.session.delete(r)
		db.session.commit()
		return "all clear"

	elif(table=='JoinRide' and column=="deleteall"):
		r=JoinRide.query.all()
		db.session.delete(r)
		db.session.commit()
		return "all clear"
	#end of updation

	#Remove User from User table (usename is given)	
	elif(table == "User" and column == "delete"):
		r = User.query.get(insert)
		db.session.delete(r)
		db.session.commit()
		return "Del"

	#Add new User to User table 	
	elif(table=="User"):
		newuser=User(username=insert[0], password=insert[1])
		try:
			db.session.add(newuser)
			db.session.commit()
		except:
			return "failed"
		return "Done"

	#Add new Ride to Ride table 	
	elif table=="Ride":
		now = strftime("%d-%m-%y:%S-%M-%H")
		now = "{}".format(now)
		new_ride = Ride(timestamp=dat_str_dattime(insert[1]), username=insert[0], source=insert[2] ,destination=insert[3])
		try:
			db.session.add(new_ride)
			db.session.commit()		
		except:
			return "failed"		
		return "Done"

	#Join existing ride / Add existing ride by adding new row to the JoinRide column 	
	elif table == "JoinRide":
		u = "{}".format(insert[1])
		try:
			join=JoinRide(rideId=insert[0], username=u)
			db.session.add(join)
			db.session.commit()
		except:
			return "failed"
		return "Done"

 #Delete Ride from Ride table (rideID is given)  
	elif((table[0] == "Ride" or table[1] == "JoinRide") and column == "delete"):
		r1 = Ride.query.get(insert)
		r2 = JoinRide.query.filter_by(rideId = insert).first()
		if r1:
			db.session.delete(r1)
		if r2:
			db.session.delete(r2)
		db.session.commit()
		return "del"


#-----------------------------------------------DB_READ OPERATIONS-------------------------------------------------------------
def DB_Read(read_data):

	# data = request.get_jason()
	data = json.loads(read_data)

	table = data["table"]
	column = data["column"]
	insert = data["insert"]
		
	db.session.rollback()
	#Read username to before deleting from User Table
	if(table == "User" and column == "delete"):	
		r = User.query.get(insert)
		if r:
			return "Yes"
		else:
			return "No"

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
			return "Yes"
		else:
			return "No"
	
	#Counting total number of rides
	elif insert == "total_rides":
		cnt = db.session.query(Ride).count()
		print(cnt)
		return str(cnt)
		
	#Read rideID before delete from Ride Table
	elif(table[0] == "Ride" or table[1] == "JoinRide" ) and (column == "delete"):	
		joinResponse = JoinRide.query.filter_by(rideId = insert).first() 
		RideResponse = Ride.query.get(insert)
		if RideResponse :
			return "InRide"
		elif joinResponse:
			return "InJoin"
		else:
			return "No"

	#Read from JoinRide table before adding new ride (given ride ID)
	elif(table == "JoinRide" ):
		ride_tab_u = JoinRide.query.get(insert[1])
		r = Ride.query.filter_by(username = insert[1]).first()
		if r:
			return "Yes"
		elif ride_tab_u:
			return "Yes"
		return "No"

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
			return "Yes"
		else:
			return "No"

#------------------------------------------------------SYNC----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# def sync():
def callback(ch, method, properties, body):
	print(" [x]slave sync")
	DB_Write(body)




#-----------------------------------------------RPC WRITE REQUEST/RESPONSE-------------------------------------------------------------------------------------------------------------------------------------
def read_request(ch, method, props, body):
	response = DB_Read(body)
	print("SLAVE")
	ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id = props.correlation_id), body=response)
	ch.basic_ack(delivery_tag=method.delivery_tag)

channel2.basic_qos(prefetch_count=1)                 #load balance among multiple servers
channel2.basic_consume(queue='readQ', on_message_callback=read_request)

channel3.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print(" [x] Awaiting READ requests")
channel2.start_consuming()
channel3.start_consuming()



