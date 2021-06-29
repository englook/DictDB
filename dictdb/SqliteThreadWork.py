#############################################################################
##            __          __  _  _______            _                      ##
##            \ \        / / | ||__   __|          | |                     ##
##             \ \  /\  / /__| |__ | |_ __ __ _  __| | ___ _ __            ##
##              \ \/  \/ / _ \ '_ \| | '__/ _` |/ _` |/ _ \ '__|           ##
##               \  /\  /  __/ |_) | | | | (_| | (_| |  __/ |              ##
##                \/  \/ \___|_.__/|_|_|  \__,_|\__,_|\___|_|              ##
##                                                                         ##
##        Copyright (c) 2020 MarcosVs98 - Todos os Direitos Reservados     ##
##                                                                         ##
#############################################################################
import os
import time
import uuid
import json
import logging
import queue
import sqlite3
import threading
from pathlib import Path
from abc import ABC, abstractmethod
from colorama import Fore, Style

log = logging.getLogger('sharedWork')


class SqliteThreadWork(threading.Thread):
	"""
		Class responsible for implementing a database
		curly based on python dictionary.
	"""
	def __init__(self, database, conn=None, max_queue_size=2000):
		threading.Thread.__init__(self)
		self.database = database
		self.daemon = True
		self.cur = None
		self.read_only = False
		self.active_context = False
		self.queue = queue.Queue(maxsize=max_queue_size)
		self.max_queue_size = max_queue_size
		self.results = {}
		self.exit_set = False
		self.exit_token = str(uuid.uuid4())
		self.conn = sqlite3.connect(
			database=database,
			uri=False,
			isolation_level='EXCLUSIVE',
			timeout=5,
			check_same_thread=False,
			detect_types=sqlite3.PARSE_DECLTYPES
		)
		self.cur = self.conn.cursor()
		self.thread_running = True
		self.start()

	def run(self):
		log.debug("run: Thread started")
		execute_count = 0

		for token, query, values in iter(self.queue.get, None):
			log.debug("queue: %s", self.queue.qsize())
			if token != self.exit_token:
				log.debug("run: %s", query)
				self.run_query(token, query, values)
				execute_count += 1
				# Let the executor grow a bit before committing to disk
				# to speed things up.
				if (self.queue.empty() or execute_count == self.max_queue_size):
					log.debug("run: commit")
					#if self.in_transaction:
					self.conn.commit()
					execute_count = 0
			# Exit only if the queue is empty. otherwise keep receiving
			# through the queue until it is empty.
			if self.exit_set and self.queue.empty():
				self.conn.commit()
				self.conn.close()
				self.thread_running = False
				return

	def run_query(self, token, query, values):
		"""
		method responsible for executing the query
			:param token: A uuid object of the query you want to return.
			:param query: A sql query with? placeholders for values.
			:param values: A tuple of values ​​to replace "?" in consultation
		"""
		if query.lower().strip().startswith("select"):
			try:
				self.cur.execute(query, values)
				self.results[token] = self.cur.fetchall()
			except sqlite3.Error as err:
				# Put the error in the output queue from a reply
				# it is necessary.
				self.results[token] = ("Query returned error: %s: %s: %s" % (query, values, err))
				log.error("Query returned error: %s: %s: %s", query, values, err)
		else:
			try:
				self.cur.execute(query, values)
			except sqlite3.Error as err:
				log.error("Query returned error: %s: %s: %s", query, values, err)

	def close(self):
		self.exit_set = True
		self.queue.put((self.exit_token, "", ""), timeout=5, block=True)
		while self.thread_running:
			# Don't kill the CPU wait.
			time.sleep(.01)

	def query_results(self, token):
		"""
		method responsible for getting query results for a specific token.

		:param token: A uuid object of the query you want to return.
		:return: Returns the results of the query when it is executed by the thread.
		"""
		delay = .001
		while True:
			if token in self.results:
				return_val = self.results[token]
				del self.results[token]
				return return_val
			# Double back on the delay to a max of 8 seconds.  This prevents
			# a long lived select statement from trashing the CPU with this
			# infinite loop as it's waiting for the query results.
			log.debug("Sleeping: %s %s", delay, token)
			time.sleep(delay)
			if delay < 10:
				delay += delay

	def execute(self, query, values=None):
		"""
		Args:
			query: String sql using? for dynamic value placeholders.
			values: A tuple of values ​​to be substituted into? of the consultation.
		Returns:
			If it's a selected query, it will return the query results.
		"""
		if self.exit_set:
			log.debug("Exit set, not running: %s", query)
			return ["Exit Called"]
		log.debug("execute: %s", query)
		values = values or []

		# A token to track this query.
		token = str(uuid.uuid4())
		# If it's a selection, we line it up with a token to mark the results
		# in the output queue to let us know what our results are.

		if query.lower().strip().startswith("select"):
			self.queue.put((token, query, values), timeout=5, block=False)
			return self.query_results(token)
		else:
			self.queue.put((token, query, values), timeout=5, block=True)

	@property
	def queue_size(self):
		"""
		Method responsible for getting the size of the execution queue
		"""
		return self.queue.qsize()

# end-of-file