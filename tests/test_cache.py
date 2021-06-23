
from lib.cache import Cache


def test_cache():
    items_to_cache = set((
        ('a', 'bla'),
        ('a', 'foo'),
        ('b', 'foo')
    ))
    cache = Cache(items_to_cache)

    data = [
        {'bla': 1, 'xyz': 2},
        {'bla': 3, 'xyz': 4}
    ]

    # Should be cached
    cache.add('a', data)

    # Should not be cached
    cache.add('b', data)

    a_bla = cache.retrieve('a.bla')
    assert a_bla == [1, 3]

    b_bla = cache.retrieve('b.bla')
    assert b_bla == []
