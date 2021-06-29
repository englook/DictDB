import os
import time
import json
import sqlite3
import threading
import settings
from pathlib import Path
from abc import ABC, abstractmethod


class WBStorageException(Exception):
	pass


class WBStorageReadOnlyException(WBStorageException):
	pass


class WBSharedStorageConnectionException(WBStorageException):
	pass


class SharedStorage(ABC):
	"""
		Class responsible for implementing a database
		curly based on python dictionary.
	"""
	def __init__(self, database, storage, read_only=False, expires=60, type_key="timestamp"):
		self.conn = None
		self.database = database
		self.storage = storage
		self.read_only = read_only
		self.expires = expires
		self.table = f'tb_{storage}'
		self.active_context = False
		self.type_key = type_key

		if read_only:
			if not Path(database).is_file():
				raise WBStorageException(
					f'database "{database}" not found.')

		self.conn = sqlite3.connect(
			database=database,
			uri=read_only,
			isolation_level='EXCLUSIVE',
			timeout=60,
			check_same_thread=True)

		self.conn.set_trace_callback(self._debug_trace)
		try:
			self._initialize()
		except sqlite3.OperationalError as e:
			raise sqlite3.Error(e)

	def _debug_trace(self, msg):
		pid = os.getpid()
		tid = threading.current_thread().name
		self.debug_trace(pid, tid, msg)

	@abstractmethod
	def debug_trace(self, pid, tid, msg):
		pass

	def _check_read_only(self):
		if self.read_only:
			raise WBStorageReadOnlyException(
				'Database/Storage in read-only mode.')

	def _initialize(self):
		self.begin()

		cur = self.conn.execute(
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?;",
			(self.table,))

		if not cur.fetchone() and self.read_only:
			self.rollback()
			raise WBStorageException(
				f'Storage "{self.storage}" not found in database "{self.database}".')

		if not self.read_only:
			self.conn.execute(f'CREATE TABLE IF NOT EXISTS {self.table} ('
			                  f'key {self.type_key} NOT NULL PRIMARY KEY, '
			                  f'value TEXT NOT NULL, '
			                  f'inserted INTEGER NOT NULL,'
			                  f'updated INTEGER NOT NULL );')
			if self.expires > 0:
				self.conn.execute(
					f'DELETE FROM {self.table} WHERE updated + ? < ?;',
					(self.expires, int(time.time())))
		self.commit()

	def count(self):
		cur = self.conn.execute(f'SELECT count(1) FROM {self.table};');
		return cur.fetchone()[0]

	def keys(self):
		cur = self.conn.execute(f'SELECT key FROM {self.table};');
		return [row[0] for row in cur.fetchall()]

	def values(self):
		cur = self.conn.execute(f'SELECT value FROM {self.table};');
		return [row[0] for row in cur.fetchall()]

	def items(self):
		cur = self.conn.execute(f'SELECT key, value FROM {self.table};');
		return [(row[0], json.loads(row[1]).get('data'))
			for row in cur.fetchall()]

	def _set(self, key, value):
		sql = f'INSERT OR REPLACE INTO {self.table} (key, value, inserted, updated) ' \
		      f'VALUES (:key, :value, COALESCE((SELECT inserted ' \
		      f'FROM {self.table} WHERE key = :key), :now), :now);'
		self.conn.execute(sql, {'key': key, 'now': int(time.time()),
			'value': json.dumps({'data': value})})

	def set(self, key, value):
		self._check_read_only()
		self._set(key, value)
		self.commit()

	def delete(self, key, ignore_key_error=False):
		self._check_read_only()
		cur = self.conn.execute(f'DELETE FROM {self.table} WHERE key = ?;', (key,))
		self.commit()
		if not cur.rowcount and not ignore_error:
			raise KeyError(f'storage key "{key}" not found.')

	def get(self, key, default=None):
		cur = self.conn.execute(f'SELECT value FROM {self.table} WHERE key = ?;', (key,))
		row = cur.fetchone()
		if not row:
			if default is None:
				raise KeyError(f'storage key "{key}" not found.')
			return default
		return json.loads(row[0]).get('data')

	def update(self, dic):
		self._check_read_only()
		for k, v in dic.items():
			self._set(k, v)
		self.commit()

	def pop(self, key, default=None):
		self._check_read_only()
		try:
			self.begin()
			value = self.get(key)
			self.delete(key, ignore_key_error=True)
			self.commit()
			return value
		except KeyError as e:
			self.rollback()
			if default is None:
				raise e
			return default

	def age(self, key):
		cur = self.conn.execute(f'SELECT inserted, updated FROM {self.table} WHERE key = ?;', (key,))
		row = cur.fetchone()
		if not row:
			raise KeyError(f'storage key "{key}" not found.')
		now = int(time.time())
		return (now - row[0], now - row[1])

	def clear(self):
		self._check_read_only()
		self.conn.execute(f'TRUNCATE TABLE {self.table};')

	def begin(self):
		if self.active_context:
			return
		if not self.conn.in_transaction:
			self.conn.execute('BEGIN EXCLUSIVE;')

	def rollback(self):
		if self.active_context:
			return
		if self.conn.in_transaction:
			self.conn.rollback()

	def commit(self):
		if self.active_context:
			return
		if self.conn.in_transaction:
			self.conn.commit()

	def close(self):
		if self.conn:
			self.conn.close()

	def __enter__(self):
		try:
			self.begin()
			self.active_context = True
		except:
			pass
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.active_context = False
		if exc_type is None:
			self.commit()
		else:
			self.rollback()
		self.conn.close()

	def __getitem__(self, key):
		return self.get(key)

	def __setitem__(self, key, value):
		return self.set(key, value)

	def __len__(self):
		return self.count()

	def __delete__(self, key):
		self.delete(key)

	def __del__(self):
		if self.conn:
			self.conn.close()

	def __repr__(self):
		return (f"{type(self).__name__}("
		        f"database='{self.database}', "
		        f"storage='{self.storage}', "
		        f"read_only={self.read_only}, "
		        f"max_age={self.expires}, "
		        f"nkeys={self.count()})")

# end-of-file