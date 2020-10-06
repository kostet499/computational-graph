import typing as tp
from . import operations as ops
from .external_sort import ExternalSort


class Graph:
    """Computational graph implementation"""

    def __init__(self, data_iter_getter: tp.Any) -> None:
        self.data_iter_getter = data_iter_getter

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        :param name: name of kwarg to use as data source
        """
        def func(**kwargs: tp.Any) -> ops.TRowsGenerator:
            return kwargs[name]()
        return Graph(func)

    @staticmethod
    def graph_iter_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> ops.TRowsGenerator:
        """Construct new graph extended with operation for reading rows from file
        :param filename: filename to read from
        :param parser: parser from string to Row
        """

        with open(filename) as f:
            for line in f:
                yield parser(line)
                # break  # read only one line for less time consumption
        print("file iteration finished")

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        def func(**kwargs: tp.Any) -> ops.TRowsGenerator:
            yield from ops.Map(mapper)(self.data_iter_getter(**kwargs))
        return Graph(func)

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        def func(**kwargs: tp.Any) -> ops.TRowsGenerator:
            yield from ops.Reduce(reducer, tp.cast(tp.Tuple[str, ...], keys))(self.data_iter_getter(**kwargs))
        return Graph(func)

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        def func(**kwargs: tp.Any) -> ops.TRowsGenerator:
            yield from ExternalSort(keys)(self.data_iter_getter(**kwargs))
        return Graph(func)

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        def func(**kwargs: tp.Any) -> ops.TRowsGenerator:
            yield from ops.Join(joiner, keys)(join_graph.data_iter_getter(**kwargs), self.data_iter_getter(**kwargs))
        return Graph(func)

    def run(self, **kwargs: tp.Any) -> tp.List[ops.TRow]:
        """Single method to start execution; data sources passed as kwargs"""
        return list(self.data_iter_getter(**kwargs))

    def run_iter(self, **kwargs: tp.Any) -> ops.TRowsGenerator:
        """Single method to start execution; data sources passed as kwargs and returned as generator"""
        return self.data_iter_getter(**kwargs)
