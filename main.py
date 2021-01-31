from pyspark import SparkContext


def main():
    sc = SparkContext(master="local[4]", appName="numbers_test")

    logData = sc.textFile('hdfs://localhost:8020/input/test.txt')
    print(logData.count())
    # numAs = logData.filter(lambda s: 'a' in s).count()
    # numBs = logData.filter(lambda s: 'b' in s).count()
    #
    # print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
