import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

def createPair(line: String): List[(String, String)] = {
	val splits = line.split("\t")
	if (splits.size > 1) {
	    val kuid = splits(0)
	    val frndList = splits(1).split(",").toList
	    val frnds = splits(1).replaceAll(",", "-")
	    var key: String = null

	    var res: List[(String, String)] = List()

	    for (f <- frndList) {
	    	if (f.toInt < kuid.toInt) {
	    		key = f + "-" + kuid
	    	} else {
	    		key = kuid + "-" + f
	    	}
	    	res = res :+ (key, frnds)
	    }
	    return res
	} else {
		var res: List[(String, String)] = List()
		return res
	}
}

def reducer(v1: String, v2: String): String = {
	val v1Set = v1.split("-").toSet
	val v2Set = v2.split("-").toSet
	val mutualFriends = v1Set.intersect(v2Set).mkString("-")
	return mutualFriends
}

def mfCount(i: (String, String)): (String, Int) = {
	val key = i._1
	val value = i._2.split("-").size
	val result = (key, value)
	return result
}

val textFile = sc.textFile("soc-LiveJournal1Adj.txt")
val userFile = sc.textFile("userdata.txt")

val inputRDD = textFile.flatMap(line => createPair(line)).reduceByKey((v1, v2) => reducer(v1, v2))

val input2RDD = userFile.map(line => line.split(",")).map(line => (line(0), (line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9))))

val temp = inputRDD.map(line => mfCount(line)).sortBy(_._2, false).take(10)
val temp1 = temp.map(f => (f._1.split("-")(0), (f._1, f._2)))
val temp2 = temp.map(f => (f._1.split("-")(1), (f._1, f._2)))

val temp1RDD = sc.parallelize(temp1)
val temp2RDD = sc.parallelize(temp2)

val userAjoin = temp1RDD.join(input2RDD).map(r => (r._2._1._1, (r._2._1._2, r._2._2)))
val userBjoin = temp2RDD.join(input2RDD).map(r => (r._2._1._1, (r._2._1._2, r._2._2)))

val userInfo = userAjoin.join(userBjoin).map(r => r._2._1._1 + "\t" + r._2._1._2._1 + "\t" + r._2._1._2._2 + "\t" + r._2._1._2._3 + "\t" + r._2._2._2._1 + "\t" + r._2._1._2._2 + "\t" + r._2._1._2._3)

userInfo.coalesce(1).saveAsTextFile("question2.out")
