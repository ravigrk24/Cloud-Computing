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
import pika
import uuid
import docker
import threading
import math
import time
from kazoo.client import KazooClient

zk = KazooClient(hosts = "zoo")

zk.start()

zk.ensure_path("/master")

data, stat = zk.get("/master")

zk.create("/master/slave",ephemeral=True)

app = Flask(__name__)

workers = []

global read_counter
read_counter = 0

global slave_cnt
slave_cnt = 0

global flag
flag = 0

#----------------------------------------------------------------docker SDK----------------------------------------------------------------------------------------
client = docker.from_env()
print("Containers list")
container_list = []
for container in client.containers.list():
	container_list.append(container.name)
	if(container.name=='slave'):
		workers.append(container.top()['Processes'][0][1])
print(container_list)
print(workers)



#-----------------------------------------------------------------------RPC READ----------------------------------------------------------------------------------------------
class Rpc_read(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq', heartbeat=0))
		self.channel2 = self.connection.channel()
		result = self.channel2.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue
		self.channel2.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

    #create callback readQ for read response...
	def read_call(self, data):
		self.response = None
		self.corr_id = str(uuid.uuid4())

		self.channel2.basic_publish(exchange='',routing_key='readQ', properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id,),body=data)
		while self.response is None:
			self.connection.process_data_events()
		return self.response

#-----------------------------------------------------------------------RPC WRITE----------------------------------------------------------------------------------------------
class Rpc_write(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq', heartbeat=0))
		self.channel1 = self.connection.channel()
		result = self.channel1.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue
		self.channel1.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

    #create callback writeQ for write response...
	def write_call(self, data):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel1.basic_publish(exchange='', routing_key='writeQ', properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id,), body=data)
		    # When a request appears, it does the job and sends a message with the result back to the Client, using the queue from the reply_to field.
		while self.response is None:
		    self.connection.process_data_events()
		return self.response



#---------------------------------------------------------------------------------Scale upDown----------------------------------------------------------------------
def scale_up_down():
	time.sleep(5)
    
	while(not(flag)):
		continue

	global read_counter
	global slave_cnt

	while(1):
		time.sleep(120)
		
		cnt = read_counter
		n = int(math.ceil(cnt/20))
		
		x = n-slave_cnt

		while(x > 0):

			
			slave_cnt += 1

			slave_name = "slave"+str(slave_cnt)
			
			ll1 = client.containers.list(filters={'name':slave_name})
			if(len(ll1)>0):
				slave_name = slave_name+"new1"+str(len(ll1))
			container = client.containers.run(image = "slave:latest", name= slave_name, command="python slave.py", network_mode= "orc_project_default",links = {"rmq":"rmq"}, detach=True)
			workers.append(container.top()['Processes'][0][1])
			print("slave spawned")
			data = "copy_db " + slave_name
			write_rpc = Rpc_write()
			print("[x] Write Request...")
			response = write_rpc.write_call(data)
			print(response)
			x -= 1

	    #Scale Down
		while(x < 0):
			s_name = "slave"+str(slave_cnt)
			#l = client.containers.list(filters={'name':s_name})
			#if(len(l) > 0):
			#	l[0].kill()
			#	print("Killed "+s_name)
			
			l =[]
			pid = []
			for container in client.containers.list():
				if(container.name != 'master' and container.name != 'orchestrator' and container.name != 'rabbitmq' and container.name != 'zoo'):
					l1 = []
					l1.append(container.top()['Processes'][0][1])
					pid.append(container.top()['Processes'][0][1])
					l1.append(container)
					l.append(l1)
	
			l = sorted(l, reverse=True)
			pid = sorted(pid, reverse=True)
			#print(l)
			#print(l[0][1])
			if(len(l)>1):
				workers.remove(pid[0]) 
				l[0][1].kill()
				
				x += 1
				slave_cnt -= 1
		
		read_counter = 0
            
            


t1 = threading.Thread(target=scale_up_down, args=())
t1.start()

#-------------------------------------------------------------------------------READ REQ-----------------------------------------------------------------------
@app.route('/api/v1/db/read', methods=['POST'])
def read_request():

	global read_counter
	read_counter += 1

	global flag
	flag = 1

	global slave_cnt
	if(slave_cnt <= 0):
		slave_cnt = 1

	data = request.get_json()
	read_data = json.dumps(data)
	read_rpc = Rpc_read()
	response = read_rpc.read_call(read_data)
	response = response.decode()
	print(response)
	print(type(response))
	#    	print(json.loads(response))
	print(jsonify(response))
	return jsonify((response)), 200

#--------------------------------------------------------------------------------WRITE REQ------------------------------------------------------------------------
@app.route('/api/v1/db/write', methods=['POST'])
def write_request():
	data = request.get_json()
	write_data = json.dumps(data)
	write_rpc = Rpc_write()
	response = write_rpc.write_call(write_data)
	response = response.decode()
	print(response)
	print(json.dumps(response))
	return jsonify(response), 200               

#--------------------------------------------------Clear DB---------------------------------------------------------------------------
@app.route('/api/v1/db/clear', methods=['POST'])
def clear_database():
	if request.method != 'POST':
		return jsonify({}),405
	data={
			"insert":"",
			"column": "",
			"table" : "clear_db"
		}

	write_data = json.dumps(data)
	write_rpc = Rpc_write()
	print("[x] Write Request...")
	response = write_rpc.write_call(write_data)
	r = response.decode()
	if(r == 'Done'):
		return jsonify(r),200
	return jsonify("fail"),500	

#-------------------------------------------------------------------------------MASTER CRASH-------------------------------------------------------------------
@app.route('/api/v1/crash/master', methods=['POST'])  
def crash_master():
	l = client.containers.list(filters={'name':'master'})
	if(len(l)>0):
		l[0].kill()
		
		print("Killed")
		return jsonify({}), 200
	return jsonify({}), 400

#----------------------------------------------------------------------------------SLAVE CRASH----------------------------------------------------------------------
@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():

	l =[]
	for container in client.containers.list():
		if(container.name != 'master' and container.name != 'orchestrator' and container.name != 'rabbitmq' and container.name != 'zoo'):
			l1 = []
			l1.append(container.top()['Processes'][0][1])
			pid =container.top()['Processes'][0][1]
			l1.append(container)
			l.append(l1)
	print(container.top())
	l = sorted(l, reverse=True)
	print(l)
	print(l[0][1])
	if(len(l)>0):
		workers.remove(pid)
		print(workers)
		l[0][1].kill()
		time.sleep(5)		
		global slave_cnt
		slave_cnt -= 1
		s_name = "slave_123"+str(slave_cnt)		
		container = client.containers.run(image = "slave:latest", name=s_name, command="python slave.py", network_mode= "orc_project_default",links = {"rmq":"rmq"}, detach=True)
		workers.append(container.top()['Processes'][0][1])
		print("slave spawned")
		data = "copy_db " + s_name
		write_rpc = Rpc_write()
		print("[x] Write Request...")
		response = write_rpc.write_call(data)
		print(response)
		return jsonify({}),200
	return jsonify({}), 400

#-------------------------------------------------------------------------------------WORKER LIST---------------------------------------------------------------------
@app.route('/api/v1/worker/list', methods=['GET'])
def worker_list():
	time.sleep(5)
	pid_list = []
	for container in client.containers.list():
		pid_list.append(container.top()['Processes'][0][1])
	return jsonify(sorted(workers)), 200

#-------------------------------------------------------------------------------------MAIN---------------------------------------------------------------------------------
if __name__ == '__main__':
	app.run(host = "0.0.0.0", port = "80", debug = True, use_reloader = False)    #(use_reloader bcz bydefault flask runs 2 times)
#	t1 = threading.Thread(target=scale_up_down, args=())


