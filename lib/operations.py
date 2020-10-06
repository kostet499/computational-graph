import heapq
import itertools
import math
import operator
import string
import typing as tp
from abc import abstractmethod, ABC
from math import radians

from dateutil import parser as dateutil_parser

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


# Operations


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    """Map operation"""
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: iterator over TRow
        :return: Generator of mapped TRow
        """
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    """Reduce operation"""
    def __init__(self, reducer: Reducer, keys: tp.Tuple[str, ...]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: iterator over TRow, sorted by self.keys
        :return: reduced groups of TRow
        """

        def get_value(row: TRow) -> tp.Optional[tp.Iterable[str]]:
            """get values in TRow pointed by key"""
            return [row[key] for key in self.keys]

        for _, g in itertools.groupby(rows, get_value):
            yield from self.reducer(self.keys, g)


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: tp.Optional[TRowsIterable], rows_b: tp.Optional[TRowsIterable]) \
            -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    """Join operation"""
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def _get_value(self, row: TRow) -> tp.List[str]:
        """get values in TRow pointed by key"""
        return [row[key] for key in self.keys]

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: iterator over TRow in left table
        :args[0] iterator over TRow in right table
        :return: generator of TRow over joined table
        """
        right_rows = iter(args[0])

        left_iterator = itertools.groupby(rows, self._get_value)
        right_iterator = itertools.groupby(right_rows, self._get_value)

        def less(left: tp.Optional[tp.List[str]], right: tp.Optional[tp.List[str]]) -> bool:
            if left is None:
                return False
            if right is None:
                return True
            return left < right

        while True:
            left_key, left_group = next(left_iterator, (None, None))
            right_key, right_group = next(right_iterator, (None, None))
            while less(left_key, right_key):
                yield from self.joiner(self.keys, left_group, None)
                left_key, left_group = next(left_iterator, (None, None))
            while less(right_key, left_key):
                yield from self.joiner(self.keys, None, right_group)
                right_key, right_group = next(right_iterator, (None, None))
            if left_key is None and right_key is None:
                break
            if left_key == right_key:
                yield from self.joiner(self.keys, left_group, right_group)


# Dummy operators

class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        first_row: tp.Union[TRow, None] = None
        for row in rows:
            if first_row is None:
                first_row = row
        assert first_row is not None
        yield first_row


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate(str.maketrans('', '', string.punctuation))
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].lower()
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""
    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        for part in row[self.column].split(self.separator):
            initial_row = row.copy()
            initial_row[self.column] = part
            yield initial_row


class Product(Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        res = 1
        for col in self.columns:
            res *= row[col]
        row[self.result_column] = res
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {key: row[key] for key in self.columns}


class Idf(Mapper):
    """Count idf metrics based on number of docs and \
            number of docs containing the word"""
    def __init__(self, doc_count: str, num_word_entries: str, text_column: str, result_column: str) -> None:
        """
        :param doc_count: name of doc_count column
        :param num_word_entries: name of word entries number column
        :param text_column: name of column with word
        :param result_colum: name of column for idf
        """
        self.doc_count = doc_count
        self.num_word_entries = num_word_entries
        self.text_column = text_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        total_doc = row[self.doc_count]
        entries_count = row[self.num_word_entries]
        word = row[self.text_column]

        result = dict()
        result[self.text_column] = word
        result[self.result_column] = math.log(total_doc / entries_count)
        yield result


class Pmi(Mapper):
    """Count pmi metrics based on frequency of word \
            in docs and total"""
    def __init__(self, doc_freq: str, total_freq: str, result_column: str) -> None:
        """
        :param doc_freq: name of doc frequency column
        :param total_freq: name of total frequency column
        :param result_colum: name of column for pmi
        """
        self.doc_freq = doc_freq
        self.total_freq = total_freq
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        doc_freq = row[self.doc_freq]
        total_freq = row[self.total_freq]
        row[self.result_column] = math.log(doc_freq / total_freq)
        yield row


class ProcessLength(Mapper):
    """Get edge length"""
    def __init__(self, start_coord_column: str, end_coord_column: str, length_column: str) -> None:
        """
        :param start_coord_column: name of start column
        :param end_coord_column: name of end column
        :param length_column: name of column for length
        """
        self.start = start_coord_column
        self.end = end_coord_column
        self.length = length_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        l1, f1 = row[self.start]
        l2, f2 = row[self.end]
        l1 = radians(l1)
        l2 = radians(l2)
        f1 = radians(f1)
        f2 = radians(f2)
        row[self.length] = 6371 * 2 * math.asin(math.sqrt(math.sin(f2/2 - f1/2) * math.sin(f2/2 - f1/2) +
                                                          math.cos(f1) * math.cos(f2) * math.sin(l2/2 - l1/2) *
                                                          math.sin(l2/2 - l1/2)))
        yield row


class ProcessTime(Mapper):
    """Get edge length"""
    def __init__(self, enter_time_column: str, leave_time_column: str,  time_column: str, weekday_column: str,
                 hour_column: str) -> None:
        """
        :param enter_time_column: name of enter time column
        :param leave_time_column: name of leave time column
        :param time_column: name of column for time
        :param weekday_column: name of column for week day
        :param hour_column: name of column for hour
        """
        self.enter = enter_time_column
        self.leave = leave_time_column
        self.time = time_column
        self.day = weekday_column
        self.hour = hour_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        date1 = dateutil_parser.parse(row[self.enter])
        date2 = dateutil_parser.parse(row[self.leave])

        row[self.day] = date1.strftime('%a')
        row[self.hour] = date1.hour
        row[self.time] = (date2 - date1).total_seconds()
        yield row


class ProcessSpeed(Mapper):
    """Get speed based on length and time"""
    def __init__(self, length_column: str, time_column: str, speed_column: str) -> None:
        """
        :param length_column: column for total length
        :param time_column: column for total time
        :param speed_column: column for redult speed
        """
        self.length = length_column
        self.time = time_column
        self.speed = speed_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.speed] = row[self.length] / row[self.time] * 3600
        yield row

# Reducers


class TopN(Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in heapq.nlargest(self.n, rows, key=operator.itemgetter(self.column_max)):
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        counts: tp.Dict[tp.Any, int] = dict()
        length = 0
        row_sample = None
        for row in rows:
            if row_sample is None:
                row_sample = row
            word = row[self.words_column]
            if word not in counts:
                counts[word] = 1
            else:
                counts[word] += 1
            length += 1

        assert row_sample is not None
        for word, count in counts.items():
            result: TRow = dict()
            for key in group_key:
                result[key] = row_sample[key]
            result[self.words_column] = word
            result[self.result_column] = count / length
            yield result


class Count(Reducer):
    """Count rows passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result: TRow = dict()
        row_sample = None
        length = 0
        for row in rows:
            if row_sample is None:
                row_sample = row
            length += 1
        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        result[self.column] = length
        yield result


class SafeCount(Reducer):
    """Count rows passed and yield multiple row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result = dict()
        row_sample = None
        length = 0
        for row in rows:
            if row_sample is None:
                row_sample = row
            length += 1

        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        result[self.column] = length
        for i in range(length):
            yield result


class Sum(Reducer):
    """Sum values in columns passed and yield single row as a result"""
    def __init__(self, columns: tp.Iterable[str]) -> None:
        """
        :param column: name of columns to sum
        """
        self.columns = columns

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result = dict()
        row_sample = None
        s = dict()

        for col in self.columns:
            s[col] = 0

        for row in rows:
            if row_sample is None:
                row_sample = row
            for col in self.columns:
                s[col] += row[col]

        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        for col in self.columns:
            result[col] = s[col]
        yield result


# Joiners
def merge(left_row: TRow, right_row: TRow, main_keys: tp.Sequence[str], suf_a: str, suf_b: str) -> TRow:
    """Auxiliary function for merging common keys in left and right rows"""
    left_keys = set(left_row.keys())
    right_keys = set(right_row.keys())
    keys = left_keys | right_keys
    result = dict()
    for key in keys:
        if key in left_keys and key in right_keys:
            if key in main_keys:
                result[key] = left_row[key]
            else:
                result[key + suf_a] = left_row[key]
                result[key + suf_b] = right_row[key]
        elif key in left_keys:
            result[key] = left_row[key]
        else:
            result[key] = right_row[key]
    return result


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: tp.Optional[TRowsIterable], rows_b: tp.Optional[TRowsIterable]) \
            -> TRowsGenerator:
        if rows_a is not None and rows_b is not None:
            left_rows = list(rows_a)
            for right_row in rows_b:
                for left_row in left_rows:
                    result = merge(left_row, right_row, keys, self._a_suffix, self._b_suffix)
                    yield result


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: tp.Optional[TRowsIterable], rows_b: tp.Optional[TRowsIterable]) \
            -> TRowsGenerator:
        if rows_a is not None and rows_b is not None:
            left_rows = list(rows_a)
            for right_row in rows_b:
                for left_row in left_rows:
                    result = merge(left_row, right_row, keys, self._a_suffix, self._b_suffix)
                    yield result
        elif rows_a is not None:
            for row in rows_a:
                yield row
        elif rows_b is not None:
            for row in rows_b:
                yield row
        else:
            raise Exception("both groups are empty")


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: tp.Optional[TRowsIterable], rows_b: tp.Optional[TRowsIterable]) \
            -> TRowsGenerator:
        if rows_a is not None and rows_b is not None:
            left_rows = list(rows_a)
            for right_row in rows_b:
                for left_row in left_rows:
                    result = merge(left_row, right_row, keys, self._a_suffix, self._b_suffix)
                    yield result
        elif rows_a is not None:
            for row in rows_a:
                yield row
        else:
            pass


class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: tp.Optional[TRowsIterable], rows_b: tp.Optional[TRowsIterable]) \
            -> TRowsGenerator:
        if rows_a is not None and rows_b is not None:
            left_rows = list(rows_a)
            for right_row in rows_b:
                for left_row in left_rows:
                    result = merge(left_row, right_row, keys, self._a_suffix, self._b_suffix)
                    yield result
        elif rows_b is not None:
            for row in rows_b:
                yield row
        else:
            pass
