from pyspark import SparkContext

#logFile = "spark-2.0.0-bin-hadoop2.7/youngg-seibertoftheisland-00-t.txt"  # Should be some file on your system
logfile = "hdfs://localhost:9000/input/youngg-seibertoftheisland-00-t.txt"
sc = SparkContext("local", "Simple App")
#text_file = sc.paralellize(logfile)
text_file = sc.textFile(logfile).cache()
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("output.txt")

sc.stop()

