#!/usr/bin/env python3
import argparse
from pyspark import SparkContext


def main(args):
    sc = SparkContext(appName='common_neighbors', master=args.master)

    lines = sc.textFile(args.input)

    def line_to_edge(line):
        s = line.split("\t")
        return int(s[0]), int(s[1])

    edges = lines.map(line_to_edge)

    def edge_to_edge_plus_reversed(edge):
        s, t = edge
        return [edge, (t, s)]

    edges = edges\
        .flatMap(edge_to_edge_plus_reversed)\
        .distinct()

    joined_edges = edges.join(edges)

    def pairs_with_common_neighbors(pair):
        source = pair[1][0]
        target = pair[1][1]
        if source < target:
            return [((source, target), 1)]
        else:
            return []

    reduced_pairs_with_common_neighbors = joined_edges\
        .flatMap(pairs_with_common_neighbors)\
        .reduceByKey(lambda a, b: a + b)

    result = reduced_pairs_with_common_neighbors\
        .leftOuterJoin(edges.map(lambda edge: (edge, edge)))\
        .filter(lambda e: e[1][1] is None)\
        .map(lambda node_object: (int(node_object[1][0]), node_object[0]))\
        .sortByKey(ascending=False)

    result = sc.parallelize(result.take(args.limit))
    result.saveAsTextFile(args.output)
    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Common Neighbors')
    parser.add_argument('--input', type=str, dest='input', help='Input dir', required=True)
    parser.add_argument('--output', type=str, dest='output', help='Output dir', required=True)
    parser.add_argument('--limit', type=int, dest='limit', help='Limit on results', required=True)
    parser.add_argument('--master', type=str, dest='master', help='Spark master', default=None)
    args = parser.parse_args()

    main(args)
