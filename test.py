import unittest

class DictDBTester(unittest.TestCase):

	def setUp(self):
		self.cache = {}

	def test_update(self):
		"""
		Updates the dictionary with the specified key-value pairs
		"""
		self.cache.update({"foobar": "hello world"})
		self.assertTrue(self.cache['foobar'])

	def test_insert(self):
		self.cache['foobar'] = "xpto"
		self.assertEqual(self.cache['foobar'], "xpto")

	def test_get(self):
		"""
		Returns the value of the specified key
		"""
		self.assertEqual(self.cache.get('foobaz'), None)

	def test_keys(self):
		"""
		Returns a list containing the dictionary's keys
		"""
		self.cache['foobar'] = 1
		self.cache['foobaz'] = 2
		self.assertListEqual(['foobar', 'foobaz'], list(self.cache.keys()))

	def test_values(self):
		"""
		Returns a list of all the values in the dictionary
		"""
		self.cache['foobar'] = 1
		self.cache['foobaz'] = 2
		self.assertListEqual([1, 2], list(self.cache.values()))

	def test_items(self):
		"""
		Returns a list containing a tuple for each key value pair
		"""
		self.cache['foobar'] = 1
		self.cache['foobaz'] = 2
		self.assertTrue(len(self.cache.items())>1)

	def test_setdefault(self):
		"""
		Returns the value of the specified key. If the key does not exist:
		insert the key, with the specified value.
		"""
		self.cache['foobar'] = 1
		self.cache.setdefault('xpty')
		self.assertEqual(self.cache['xpty'], None)

	def test_copy(self):
		"""
		Returns a copy of the dictionary.
		"""
		self.new_cache = self.cache.copy()
		self.assertDictEqual(self.cache, self.new_cache)

	def test_pop(self):
		"""
		Removes the element with the specified key
		"""
		self.cache['foobar'] = 1
		self.cache.pop('foobar')
		self.assertFalse('foobar' in self.cache)

	def test_popitem(self):
		"""
		Removes the last inserted key-value pair
		"""
		self.cache['foobar'] = 1
		self.cache.popitem()
		self.assertTrue(True)

	def test_fromkeys(self):
		"""
		Returns a dictionary with the specified keys and value
		"""

		self.cache = self.cache.fromkeys(['a', 'b', 'c'], 1)
		self.assertListEqual([1, 1, 1], list(self.cache.values()))

	def test_clear(self):
		"""
		Removes all the elements from the dictionary
		"""
		self.cache['foobar'] = 1
		self.cache['foobaz'] = 1
		self.cache.clear()
		self.assertDictEqual({}, self.cache)

if __name__ == '__main__':
   unittest.main(verbosity=4)

# end-of-file