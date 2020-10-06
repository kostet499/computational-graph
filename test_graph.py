from .lib import Graph, operations


def test_map() -> None:
    input_stream_name = 'docs'
    text_column = 'text'

    g = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \

    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    expected = [
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 1, 'text': 'world'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'hell'}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected


def test_sort() -> None:
    input_stream_name = 'docs'
    text_column = 'text'

    g = Graph.graph_from_iter(input_stream_name) \
        .sort([text_column])

    docs = [
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 1, 'text': 'world'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'hell'}
    ]

    expected = [
        {'doc_id': 2, 'text': 'hell'},
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 1, 'text': 'world'}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected


def test_reduce() -> None:
    input_stream_name = 'docs'
    text_column = 'text'
    count_column = 'count'

    g = Graph.graph_from_iter(input_stream_name) \
        .reduce(operations.Count(count_column), [text_column]) \

    docs = [
        {'doc_id': 2, 'text': 'hell'},
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 1, 'text': 'world'}
    ]

    expected = [
        {'text': 'hell', 'count': 1},
        {'text': 'hello', 'count': 2},
        {'text': 'little', 'count': 3},
        {'text': 'my', 'count': 2},
        {'text': 'world', 'count': 1}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected
