#!/usr/bin/env python3
import argparse
from pyspark import SparkContext


def main(args):
    def debug(x):
        print(x)

    sc = SparkContext(appName='common_neighbors', master=args.master)

    lines = sc.textFile(args.input)

    def line_to_edge(line):
        s = line.split("\t")
        return int(s[0]), int(s[1])

    edges = lines.map(line_to_edge)

    def edge_to_edge_plus_reversed(edge):
        s, t = edge
        return [edge, (t, s)]

    edges = edges.flatMap(edge_to_edge_plus_reversed).distinct()

    joined_edges = edges.join(edges).distinct()

    def pairs_with_common_neighbors(pair):
        source = pair[1][0]
        target = pair[1][1]
        if source < target:
            return [((source, target), 1)]
        else:
            return []

    pairs_with_common_neighbors = joined_edges.flatMap(pairs_with_common_neighbors)

    reduced_pairs = pairs_with_common_neighbors.reduceByKey(lambda a, b: a + b)

    def test(t):
        node = t[0]
        akmi = t[1][0]
        haveNeighbor = t[1][1][0]
        haveNeighborReversed = (haveNeighbor[1], haveNeighbor[0])
        if akmi != haveNeighbor and akmi != haveNeighborReversed:
            return True
        return False

    fuckedEdges = edges.map(lambda edge: (edge, edge))
    # result = edges.map(lambda edge: (edge[0], edge)).leftOuterJoin()join(reduced_pairs.map(lambda e: (e[0][0], e))).filter(test).map(lambda element: element[1][1]).distinct()
    result = reduced_pairs.leftOuterJoin(fuckedEdges).distinct().filter(lambda e: e[1][1] is None).map(lambda makrinar: (makrinar[0], makrinar[1][0])).sortBy(lambda a, ascending=False: a[1])
    # result.foreach(debug)
    # # reduced_pairs.foreach(debug)

    # result = sc.parallelize(reduced_pairs.take(args.limit))
    # result.saveAsTextFile(args.output)
    result.saveAsTextFile(args.output)
    # reduced_pairs.foreach(debug)
    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Common Neighbors')
    parser.add_argument('--input', type=str, dest='input', help='Input dir', required=True)
    parser.add_argument('--output', type=str, dest='output', help='Output dir', required=True)
    parser.add_argument('--limit', type=int, dest='limit', help='Limit on results', required=True)
    parser.add_argument('--master', type=str, dest='master', help='Spark master', default=None)
    args = parser.parse_args()

    main(args)
