
from lib.cache import Cache


def test_cache():
    items_to_cache = set((
        ('a.1.x', 'bla.foo.daslö'),
        ('a.40.y', 'foo.123.dlkas'),
        ('b.2.y', 'foo.0ß194.111')
    ))
    cache = Cache(items_to_cache)

    data = [
        {'x': 1, 'y': 2},
        {'x': 3, 'y': 4}
    ]

    # Should be cached
    cache.add('a.1', data)

    # Should not be cached
    cache.add('b.1', data)

    a_bla = cache.retrieve('a.1.x')
    assert a_bla == [1, 3]

    b_bla = cache.retrieve('b.1.y')
    assert b_bla == []


def test_cache_no_columns():
    items_to_cache = set()
    cache = Cache(items_to_cache)

    data = [
        {'bla': 1, 'xyz': 2},
        {'bla': 3, 'xyz': 4}
    ]

    # Should not be cached
    cache.add('a', data)

    a_bla = cache.retrieve('a.bla')
    assert a_bla == []
