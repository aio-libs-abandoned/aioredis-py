import unittest

from aioredis.util import coerced_keys_dict


class CoercedKeysDictTest(unittest.TestCase):

    def test_simple(self):
        d = coerced_keys_dict()
        self.assertEqual(d, {})

        d = coerced_keys_dict({b'a': 'b', b'c': 'd'})
        self.assertIn('a', d)
        self.assertIn(b'a', d)
        self.assertIn('c', d)
        self.assertIn(b'c', d)
        self.assertEqual(d, {b'a': 'b', b'c': 'd'})

    def test_invalid_init(self):
        d = coerced_keys_dict({'foo': 'bar'})
        self.assertEqual(d, {'foo': 'bar'})

        self.assertNotIn('foo', d)
        self.assertNotIn(b'foo', d)
        with self.assertRaises(KeyError):
            d['foo']
        with self.assertRaises(KeyError):
            d[b'foo']

        d = coerced_keys_dict()
        d.update({'foo': 'bar'})
        self.assertEqual(d, {'foo': 'bar'})

        self.assertNotIn('foo', d)
        self.assertNotIn(b'foo', d)
        with self.assertRaises(KeyError):
            d['foo']
        with self.assertRaises(KeyError):
            d[b'foo']

    def test_valid_init(self):
        d = coerced_keys_dict({b'foo': 'bar'})
        self.assertEqual(d, {b'foo': 'bar'})
        self.assertIn('foo', d)
        self.assertIn(b'foo', d)
        self.assertEqual(d['foo'], 'bar')
        self.assertEqual(d[b'foo'], 'bar')

        d = coerced_keys_dict()
        d.update({b'foo': 'bar'})
        self.assertEqual(d, {b'foo': 'bar'})
        self.assertIn('foo', d)
        self.assertIn(b'foo', d)
        self.assertEqual(d['foo'], 'bar')
        self.assertEqual(d[b'foo'], 'bar')
