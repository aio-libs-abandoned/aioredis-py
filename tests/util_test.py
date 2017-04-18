from aioredis.util import decode


def test_docode():
    encoding = 'utf-8'
    assert decode(b'bytes', encoding) == 'bytes'
    assert decode([b'a', b'list'], encoding) == ['a', 'list']
    assert decode({b'a': b'dict'}, encoding) == {'a': 'dict'}
    assert decode('text', encoding) == 'text'
    assert decode([
        {
            b'key_1': [b'item_1', {b'key_2': b'val_2'}, 'item_2']
        }
    ], encoding) == [
        {
            'key_1': ['item_1', {'key_2': 'val_2'}, 'item_2']
        }
    ]
