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

def mfCount(i: (String, String)): String = {

	val key = i._1.replaceAll("-", ",")
	val value = i._2.split("-").size

	val result: String = key + "\t" + value

	return result
}

val sc = new SparkContext(new SparkConf().setAppName("question1"))
val textFile = sc.textFile("soc-LiveJournal1Adj.txt")

val inputRDD = textFile.flatMap(line => createPair(line)).reduceByKey((v1, v2) => reducer(v1, v2))

val result = inputRDD.map(line => mfCount(line))

result.coalesce(1).saveAsTextFile("question1.out")
