from .huge_graph_example import run_word_count, run_tf_idf, run_pmi, run_yandex_maps
import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description='run huge examples')
    parser.add_argument('--experiment', type=str, required=True, choices=['count', 'idf', 'pmi', 'maps'])

    args = parser.parse_args()
    if args.experiment == 'count':
        run_word_count()
    elif args.experiment == 'idf':
        run_tf_idf()
    elif args.experiment == 'pmi':
        run_pmi()
    elif args.experiment == 'maps':
        run_yandex_maps()
    else:
        raise Exception("unknown experiment name")


if __name__ == '__main__':
    main()
