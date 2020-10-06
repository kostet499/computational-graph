import itertools
import json
import os

from . import graphs
from .lib import operations as ops


def parser(line: str) -> ops.TRow:
    return json.loads(line)


dir_path = os.path.dirname(os.path.realpath(__file__))


def run_word_count() -> None:
    graph = graphs.word_count_graph('docs', text_column='text', count_column='count')
    filename = os.path.join(dir_path, "resource/text_corpus.txt")
    docs = graph.graph_iter_from_file(filename=filename, parser=parser)

    result = graph.run_iter(docs=lambda: docs)
    for res in itertools.islice(result, 5):
        print(res)

    print("experiment successfully finished")


def run_tf_idf() -> None:
    graph = graphs.inverted_index_graph('texts', doc_column='doc_id', text_column='text', result_column='tf_idf')
    filename = os.path.join(dir_path, "resource/text_corpus.txt")
    rows = graph.graph_iter_from_file(filename=filename, parser=parser)

    result = graph.run_iter(texts=lambda: iter(rows))
    for res in itertools.islice(result, 5):
        print(res)
    print("experiment successfully finished")


def run_pmi() -> None:
    graph = graphs.pmi_graph('texts', doc_column='doc_id', text_column='text', result_column='pmi')
    filename = os.path.join(dir_path, "resource/text_corpus.txt")
    rows = graph.graph_iter_from_file(filename=filename, parser=parser)

    result = graph.run_iter(texts=lambda: iter(rows))
    for res in itertools.islice(result, 5):
        print(res)
    print("experiment successfully finished")


def run_yandex_maps() -> None:
    graph = graphs.yandex_maps_graph(
        'travel_time', 'edge_length',
        enter_time_column='enter_time', leave_time_column='leave_time', edge_id_column='edge_id',
        start_coord_column='start', end_coord_column='end',
        weekday_result_column='weekday', hour_result_column='hour', speed_result_column='speed'
    )

    road_filename = os.path.join(dir_path, "resource/road_graph_data.txt")
    lengths = graph.graph_iter_from_file(filename=road_filename, parser=parser)
    time_filename = os.path.join(dir_path, "resource/travel_times.txt")
    times = graph.graph_iter_from_file(filename=time_filename, parser=parser)

    result = graph.run_iter(travel_time=lambda: times, edge_length=lambda: iter(lengths))

    for res in itertools.islice(result, 5):
        print(res)
    print("experiment successfully finished")
