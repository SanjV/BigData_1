val business = sc.textFile("business.csv")
val review = sc.textFile("review.csv")

val businessData = business.map(line => line.split("\\::")).filter(line => line(1).contains("Palo Alto")).map(line => (line(0), line(1)))
val reviewData = review.map(line => line.split("\\::")).map(line => (line(2),(line(1),line(3))))

val result = businessData.join(reviewData).distinct().map(r => r._2._2._1 + "\t" + r._2._2._2)
result.coalesce(1).saveAsTextFile("question4.out")
