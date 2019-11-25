#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4
import argparse
import os
import time
import signal
import sys
import threading

from hdbcli import dbapi

class Brute(threading.Thread):
	threads = []
	def __init__(self, hostname, port, username, password, delay_milli):
		threading.Thread.__init__(self)
		self.abort = False
		self.hostname = hostname
		self.port = port
		self.username = username
		self.password = password
		self.delay_milli = delay_milli

	def run(self):
		while not self.abort:
			try:
				new_conn = dbapi.connect(self.hostname, self.port, self.username, self.password)
				cursor = new_conn.cursor()
				cursor.execute("SELECT * FROM M_DATABASE")
				time.sleep(self.delay_milli / 1000)
				cursor.close()
				new_conn.rollback()
				new_conn.close()

			except dbapi.Error, err:
				err_str = str(err)
				print "Unexpected exception - %s" % err_str

	def stop(self):
		self.abort = True

class Confs:
	def __init__(self):
		self.args = self.parse_command_line()

	def parse_command_line(self):
		parser = argparse.ArgumentParser(description="""Make connection and disconnection in brute force way""")
		parser.add_argument('--hostname', required=False, default="localhost", type=str, help="HANA server's hostname")
		parser.add_argument('--port', required=True, type=int, help="HANA server's sql port")
		parser.add_argument('--username', required=True, type=str, help="Username")
		parser.add_argument('--password', required=True, type=str, help="Password")
		parser.add_argument('--threads', required=False, default=10, type=int, help="Number of threads")
		parser.add_argument('--delay', required=False, default=1000, type=int, help="Delay(millisecond) between consecutive connection in each threads")

		args = parser.parse_args()

		return args

def signal_handler(signal, frame):
	print('Aborting Brute Connection')
	for i in Brute.threads:
		i.stop()

	for i in Brute.threads:
		i.join()

	sys.exit(0)

def main():
	signal.signal(signal.SIGINT, signal_handler)

	conf = Confs().args

	print conf

	try:
		for i in range(0,9):
			thread = Brute(conf.hostname, conf.port, conf.username, conf.password, conf.delay)
			thread.start()
			Brute.threads.append(thread)
	except Exception as e:
		print "Error: unable to start thread"
		print e
		sys.exit(0)

	signal.pause()

if __name__ == "__main__":
	main()
