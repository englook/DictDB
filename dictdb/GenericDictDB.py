import logging
from colorama import Fore, Style
from dictdb.StorageDict import SharedStorage

log = logging.getLogger('GenericDB')


class GenericDictDB(SharedStorage):
	"""
	Generic class responsible for implementing a concrete
	SharedStorage class.
	"""
	def __init__(self, storage='generic_storage'):
		SharedStorage.__init__(self, database='generic.db', storage=storage, type_key='text')

	def save(self, key, value):
		self[key] = value

	def load(self, key):
		try:
			return self[key]
		except KeyError:
			return None

	def debug_trace(self, pid, tid, msg):
		log.info(f'{Fore.RED}[{pid}][{tid}]: {msg}{Fore.RESET}')


# end-of-file