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
#------------------------------------------------CONNECTING TO RABBITMQ-------------------------------------------------------------------

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))

channel1 = connection.channel()

channel2 = connection.channel()

channel3 = connection.channel()		#for syncQ



channel1.queue_declare(queue='writeQ')

channel2.queue_declare(queue='readQ')

# channel3.queue_declare(queue='syncQ', durable=True)

channel3.exchange_declare(exchange='logs', exchange_type='fanout')





#------------------------------------------------DB_WRITE OPERATIONS--------------------------------------------------------------------------

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

		except Exception as e:

			db.session.rollback()

			print(e)

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

			return {}

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
			db.session.commit()
		if r2:
			db.session.delete(r2)
			db.session.commit()
		return "del"

#----------------------------------------------------COPY DB TO SLAVE--------------------------------------------------------------------------

def copy_db_to_slave(slave_name):
	import docker
	import tarfile
	client = docker.from_env()

	def copy_to(src, dst):
		name, dst = dst.split(':')
		container = client.containers.get(name)

		os.chdir(os.path.dirname(src))
		srcname = os.path.basename(src)
		tar = tarfile.open(src + '.tar', mode='w')
		try:
			tar.add(srcname)
		finally:
			tar.close()

		data = open(src + '.tar', 'rb').read()
		container.put_archive(os.path.dirname(dst), data)

	copy_to('/code/db.sqlite', slave_name+':/code/db.sqlite')
	return "Copied"

	
#-------------------------------------------------SYNCQ--------------------------------------------------------------------------------------------------------

def sync(data):

	channel3.basic_publish(exchange='logs', routing_key='', body=data)

	print(" [x] Sent to syncQ")

	

#-----------------------------------------------RPC WRITE REQUEST/RESPONSE---------------------------------------------------------------------

def write_request(ch, method, props, body):

	if(body.split()[0].decode() == "copy_db"):
		response=copy_db_to_slave(body.split()[1].decode())
	else:
		response=DB_Write(body)
		sync(body)

   	#print("MASTER")
	ch.basic_publish(exchange='', routing_key=props.reply_to , properties=pika.BasicProperties(correlation_id = props.correlation_id), body=response)
	ch.basic_ack(delivery_tag=method.delivery_tag)

channel1.basic_qos(prefetch_count=1)                 #load balance among multiple servers
channel1.basic_consume(queue='writeQ', on_message_callback=write_request)

print(" [x] Awaiting WRITE requests")
channel1.start_consuming()








