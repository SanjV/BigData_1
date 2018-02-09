val business = sc.textFile("business.csv").map(line => line.split("::")).map(line => (line(0), (line(1), line(2))))


val review = sc.textFile("review.csv").map(line => line.split("::")).map(line => (line(2), line(3).toDouble)).groupByKey.mapValues(_.toList).map(x => (x._1, x._2.size))


val result = review.join(business).distinct().sortBy(_._2._1, false).take(10).map(l => l._1 + "\t" + l._2._2._1 + "\t" + l._2._2._2 + "\t" + l._2._1)


sc.parallelize(result).coalesce(1).saveAsTextFile("question3.out")
