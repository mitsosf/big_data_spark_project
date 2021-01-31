#!/usr/bin/env python3
import argparse
from pyspark import SparkContext


def main(args):
    sc = SparkContext(appName='fof', master=args.master)

    lines = sc.textFile(args.input)

    def line_to_edge(line):
        s = line.split(" ")
        return (int(s[0]), int(s[1]))

    edges = lines.map(line_to_edge)

    def edge_to_edge_plus_reversed(edge):
        s, t = edge
        return [edge, (t, s)]

    edges = edges.flatMap(edge_to_edge_plus_reversed)

    join = edges.join(edges)

    def extract_fof(tuple):
        source = tuple[1][0]
        target = tuple[1][1]
        if (source < target):
            return [(source, target)]
        else:
            return []

    fof = join.flatMap(extract_fof).distinct()

    fof.saveAsTextFile(args.output)

    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='friend of a friend')
    parser.add_argument('--input', type=str, dest='input', help='Input dir', required=True)
    parser.add_argument('--output', type=str, dest='output', help='Output dir', required=True)
    parser.add_argument('--master', type=str, dest='master', help='Spark master', default=None)
    args = parser.parse_args()

    main(args)
