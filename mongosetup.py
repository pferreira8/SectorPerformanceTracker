#Remote Connection MongoDB Setup
import time
import logging
import pymongo 
import os.path
from marketdb import MongoSectorPerformance

class MDBHandler:
	def __init__(self):
		creds = open(os.path.dirname(__file__)+'/../credentials.txt')
		conn_str = str(creds.read())
		#connection string is read from credentials.txt on local machine
		client = pymongo.MongoClient(conn_str, event_listeners=[CommandLogger()])
		self.CommandLogger(self.requestInterface(client))

	def requestInterface(self, client_interface:type(pymongo.database.Database)):
		MongoSectorPerformance(client_interface)

class CommandLogger(pymongo.monitoring.CommandListener):
	def started(self, event):
		logging.info("Command {0.command_name} with request id "
					 "{0.request_id} started on server "
					 "{0.connection_id}".format(event))

	def succeeded(self, event):
		logging.info("Command {0.command_name} with request id "
					 "{0.request_id} on server {0.connection_id} "
					 "succeeded in {0.duration_micros} "
					 "microseconds".format(event))

	def failed(self, event):
		logging.info("Command {0.command_name} with request id "
					 "{0.request_id} on server {0.connection_id} "
					 "failed in {0.duration_micros} "
					 "microseconds".format(event))


if __name__ == "__main__":
	t = time.time()
	pymongo.monitoring.register(CommandLogger())
	MDBHandler()
	print('Elapsed: %.3f seconds' % (time.time() - t))

