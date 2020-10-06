# Tiny library for graph computation

## Calculation graph interface

 The computation graph consists of entry points for data and operations on them.

 This is how it may look (although who I am trying to fool, it is - see [`graphs.py`] (graphs.py)) graph,
 which counts the number of words in documents:
 ```python
 graph = Graph.graph_from_iter('texts') \
     .map(operations.FilterPunctuation('text')) \
     .map(operations.LowerCase('text')) \
     .map(operations.Split('text')) \
     .sort(['text']) \
     .reduce(operations.Count('count'), ['text']) \
     .sort(['count', 'text'])
 ```

## How to use the library
 * create an object of one of the graph, for example: `graph = graphs.word_count_graph ('docs', text_column = 'text', count_column = 'count')`.
  In the constructor `* args` are responsible for the names of the iterators for the` run` method, `** kwargs` for the names of the columns
* supply an iterator to the input for a table like `tp.List (tp.Dict [str, tp.Any])`: `result = graphs.run (docs = lambda: iter (docs))`
* for "less memory consumption" you can enter the name of the file with the table, then you need to run it as: `result = graphs. run_iter (docs = lambda: docs) `.
* at the output, we get a list with the lines of the output table in the first case, or an iterator on them in the second.

```(python)
def parser(line: str) -> ops.TRow:
    return json.loads(line)


def run_word_count() -> None:
    graph = graphs.word_count_graph('docs', text_column='text', count_column='count')

    docs = graph.graph_iter_from_file(filename="./compgraph/resource/text_corpus.txt", parser=parser)

    result = graph.run_iter(docs=lambda: docs)
    for res in itertools.islice(result, 5):
        print(res)

    print("experiment successfully finished")
```

## Launching large tasks
 * To run, you need to pass the name of one experiment to `__main__` (run `--help` to see available experiments:
```
(shad) ➜  yagudinamir git:(compgraph) ✗ python3 -m compgraph --experiment count
file iteration finished
{'text': '00', 'count': 1}
{'text': '01', 'count': 1}
{'text': '02', 'count': 1}
{'text': '03', 'count': 1}
{'text': '04', 'count': 1}
experiment successfully finished
```  
* To run library tests:
(shad) ➜  yagudinamir git:(compgraph) ✗ pytest compgraph
