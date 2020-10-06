from . import operations as ops
from pytest import approx


def test_filter_punctuation_map() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'one,two,three!!!'},
        {'test_id': 2, 'text': 'Python 3.x.y, pytest-5.x.y, py-1.x.y, pluggy-0.x.y'}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'onetwothree'},
        {'test_id': 2, 'text': 'Python 3xy pytest5xy py1xy pluggy0xy'}
    ]

    result = list(ops.Map(ops.FilterPunctuation('text'))(iter(tests)))

    assert etalon == result


def test_split() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'camel Case Test'},
        {'test_id': 2, 'text': 'UPPER CASE TEST'},
        {'test_id': 3, 'text': 'All work and no play makes Jack a dull boy'}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'camel'},
        {'test_id': 1, 'text': 'Case'},
        {'test_id': 1, 'text': 'Test'},
        {'test_id': 2, 'text': 'UPPER'},
        {'test_id': 2, 'text': 'CASE'},
        {'test_id': 2, 'text': 'TEST'},
        {'test_id': 3, 'text': 'All'},
        {'test_id': 3, 'text': 'work'},
        {'test_id': 3, 'text': 'and'},
        {'test_id': 3, 'text': 'no'},
        {'test_id': 3, 'text': 'play'},
        {'test_id': 3, 'text': 'makes'},
        {'test_id': 3, 'text': 'Jack'},
        {'test_id': 3, 'text': 'a'},
        {'test_id': 3, 'text': 'dull'},
        {'test_id': 3, 'text': 'boy'}
    ]

    result = list(ops.Map(ops.Split(column='text'))(tests))

    assert etalon == result


def test_idf() -> None:
    tests: ops.TRowsIterable = [
        {'text': 'hello', 'doc_count': 6, 'num_word_entries': 4},
        {'text': 'little', 'doc_count': 6, 'num_word_entries': 4},
        {'text': 'world', 'doc_count': 6, 'num_word_entries': 4},
    ]

    etalon: ops.TRowsIterable = [
        {'text': 'hello', 'idf': approx(0.4054651081081644, 0.001)},
        {'text': 'little', 'idf': approx(0.4054651081081644, 0.001)},
        {'text': 'world', 'idf': approx(0.4054651081081644, 0.001)}
    ]

    result = list(ops.Map(ops.Idf('doc_count', 'num_word_entries', 'text', 'idf'))(tests))

    assert etalon == result


def test_length() -> None:
    tests: ops.TRowsIterable = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057}
    ]

    etalon: ops.TRowsIterable = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057, 'length': approx(0.032013838763095555, 0.001)}
    ]

    result = list(ops.Map(ops.ProcessLength('start', 'end', 'length'))(tests))

    assert etalon == result


def test_time() -> None:
    tests: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057}
    ]

    etalon: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057, 'weekday': 'Fri', 'hour': 11, 'time': approx(1.296, 0.001)},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057, 'weekday': 'Wed', 'hour': 14, 'time': approx(1.083, 0.001)}
    ]

    result = list(ops.Map(ops.ProcessTime('enter_time', 'leave_time', 'time', 'weekday', 'hour'))(tests))

    assert etalon == result


def test_speed() -> None:
    tests: ops.TRowsIterable = [
        {'weekday': 'Fri', 'hour': 8, 'time': 2.63, 'length': 0.045449856626228434}
    ]

    etalon: ops.TRowsIterable = [
        {'weekday': 'Fri', 'hour': 8, 'time': 2.63, 'length': 0.045449856626228434,
         'speed': approx(62.212731503582646, 0.001)}
    ]

    result = list(ops.Map(ops.ProcessSpeed('length', 'time', 'speed'))(tests))

    assert etalon == result
