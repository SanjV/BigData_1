var business = sc.textFile("business.csv")

var review = sc.textFile("review.csv")


val businessData = business.map(line => line.split("\\::")).filter(line => line(1).contains("Palo Alto")).map(line => (line(0), line(1)))

val reviewData = review.map(line => line.split("\\::")).map(line => (line(2),(line(1),line(3))))

var businessDF = businessData.toDF("bid")
var reviewDF = reviewData.toDF("uid","bid","rat")

var output = reviewDF.join(businessDF,"bid").distinct().select("uid","rat")
output.rdd.repartition(1).saveAsTextFile("question4_sql.out")

output.rdd.count()  // to calculate number of entries

