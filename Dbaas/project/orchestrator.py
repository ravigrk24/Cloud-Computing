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
from kazoo.client import KazooClient

client = docker.from_env()

zk = KazooClient(hosts = 'zoo')
zk.start()

zk.ensure_path("/master")

if zk.exists("/master/slave"):
	pass
else:
	zk.create("/master/slave",ephemeral=True)

containers = client.containers.list()


global crash
crash = False
for container in containers:
	if(container.name =='slave'):
		data,stat = zk.get('/master/slave')
		stat.pid =  container.top()['Processes'][0][1][0][1]

app = Flask(__name__)

global read_counter
read_counter = 0

global slave_cnt
slave_cnt = 0

global flag
flag = 0

@zk.ChildrenWatch("master")
def watch_children(children):
	global crash
	global slave_cnt
	if crash:
		slave_cnt += 1
		slave_name = "slave"+str(slave_cnt)
		container = client.containers.run(image = "slave:latest", name= slave_name, command="python slave.py", network_mode= "test_default",links = {"rmq":"rmq"}, detach=True)
		pid = container.top()['Processes'][0][1][0][1]
		zk.create('/master/'+slave_name)
		data,stat = zk.get('/master/'+slave_name)
		stat.pid = pid
		print("slave spawned by watcher")
	crash=False


#----------------------------------------------------------------docker SDK----------------------------------------------------------------------------------------

print("Containers list")
container_list = []
for container in client.containers.list():
	container_list.append(container.name)
print(container_list)



#-----------------------------------------------------------------------RPC READ----------------------------------------------------------------------------------------------
class Rpc_read(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
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
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
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

#---------------------------------------------------------------------------------Scale up/Down----------------------------------------------------------------------
def scale_up_down():
	
	while(not(flag)):
		continue

	global read_counter
	global slave_cnt
	global crash
	crash = False
	while(1):
		time.sleep(10)
		
		cnt = read_counter
		n = int(math.ceil(cnt/4))
		x = n-slave_cnt

		while(x > 0):

			
			slave_cnt += 1

			slave_name = "slave"+str(slave_cnt)
			container = client.containers.run(image = "slave:latest", name= slave_name, command="python slave.py", network_mode= "test_default",links = {"rmq":"rmq"}, detach=True)
			pid = container.top()['Processes'][0][1][0][1]
			zk.create('/master/'+slave_name)
			data,stat = zk.get('/master/'+slave_name)
			stat.pid = pid
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
			l = client.containers.list(filters={'name':s_name})
			if(len(l) > 0):
				l[0].kill()
				zk.delete('/master/'+s_name)
				print("Killed "+s_name)
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
	if(slave_cnt == 0):
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

#-------------------------------------------------------------------------------MASTER CRASH-------------------------------------------------------------------
@app.route('/api/v1/crash/master', methods=['POST'])  
def crash_master():
	l = containers.list(filters={name:'master'})
	l[0].kil()
	print("Killed")
	return jsonify({}), 201

#----------------------------------------------------------------------------------SLAVE CRASH----------------------------------------------------------------------
@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	global crash
	global slave_cnt
	crash = True
	l =[]
	for container in client.containers.list():
		if(container.name != 'master' and container.name != 'orchestrator' and container.name != 'rabbitmq' and container.name != 'zoo'):
			l1 = []
			l1.append(container.top()['Processes'][0][1])
			l1.append(container)
			l.append(l1)
	print(container.top())
	l = sorted(l, reverse=True)
	children = zk.get_children('/master')
	for child in children:
		if child.pid == l[0][1]:
			zk.delete('/master/'+child)
			slave_cnt-=1
	print(l)
	print(l[0][1])
	l[0][1].kill()
	
	return jsonify({}),201

#-------------------------------------------------------------------------------------WORKER LIST---------------------------------------------------------------------
@app.route('/api/v1/worker/list', methods=['POST'])
def worker_list():
	pid_list = []
	for container in client.containers.list():
		pid_list.append(container.top()['Processes'][0][1])
	return jsonify(sorted(pid_list)), 201

#-------------------------------------------------------------------------------------MAIN---------------------------------------------------------------------------------
if __name__ == '__main__':
	app.run(host = "0.0.0.0", port = "80", debug = True, use_reloader = False)    #(use_reloader bcz bydefault flask runs 2 times)
#	t1 = threading.Thread(target=scale_up_down, args=())
	zk.stop()
