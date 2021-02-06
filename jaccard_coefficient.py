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

    def res_to_res_plus_reversed(res):
        neighbors = res[0]
        s, t = res[1]
        return [res, (neighbors, (t, s))]

    joined_results = result.flatMap(res_to_res_plus_reversed)

    def fix_keys(key):
        source = key[0][0]
        target = key[0][1]
        count = key[1]
        neighbor = key[2]
        if int(source) > int(target):
            return (target, source), count, neighbor
        return key

    def finalMap(key):
        source = key[0][0]
        target = key[0][1]
        common_neighbors = key[0][2]
        all_neighbors = key[1]

        return float(common_neighbors)/float(all_neighbors), (source, target)

    neighbors = joined_results\
        .map(lambda obj: (obj[1][0], (obj[1][1], obj[0])))\
        .join(edges)\
        .map(lambda res: ((res[0], res[1][0][0]), res[1][0][1], res[1][1]))\
        .map(fix_keys)\
        .distinct()\
        .map(lambda obj: ((obj[0][0], obj[0][1], obj[1]), 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(finalMap)\
        .sortByKey(ascending=False)

    neighbors.foreach(debug)
    neighbors = sc.parallelize(neighbors.take(args.limit))
    # neighbors.saveAsTextFile(args.output)
    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Common Neighbors')
    parser.add_argument('--input', type=str, dest='input', help='Input dir', required=True)
    parser.add_argument('--output', type=str, dest='output', help='Output dir', required=True)
    parser.add_argument('--limit', type=int, dest='limit', help='Limit on results', required=True)
    parser.add_argument('--master', type=str, dest='master', help='Spark master', default=None)
    args = parser.parse_args()

    main(args)
