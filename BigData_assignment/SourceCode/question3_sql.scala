import spark.implicits._

val business = sc.textFile("business.csv").map(line => line.split("::")).map(line => (line(0), line(1), line(2)))
val review = sc.textFile("review.csv").map(line => line.split("::")).map(line => (line(2), line(3).toDouble)).groupByKey.mapValues(_.toList).map(x => (x._1, x._2.size))

var businessDF = business.toDF("bid", "add", "cat")
var reviewDF = review.toDF("bid", "num")

var output = reviewDF.join(businessDF,"bid").distinct().select("bid", "add", "cat", "num").orderBy($"num".desc).limit(10)

output.rdd.repartition(1).saveAsTextFile("question3_sql.out")
