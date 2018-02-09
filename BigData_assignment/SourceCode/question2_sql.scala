import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray

def createPair(line: String): List[(String, String, List[String])] = {
	val splits = line.split("\t")
	if (splits.size > 1) {
	    val kuid = splits(0)
	    val frndList = splits(1).split(",").toList
	    var res: List[(String, String, List[String])] = List()

	    for (f <- frndList) {
	    	res = res :+ (kuid, f, frndList)
	    }
	    return res
	} else {
		var res: List[(String, String, List[String])] = List()
		return res
	}
}

val conf = new SparkConf().setMaster("local[*]").setAppName("q1sql")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)

val textFile = sc.textFile("soc-LiveJournal1Adj.txt")
val inputRDD = textFile.flatMap(line => createPair(line))

val table1 = inputRDD.toDF("uid", "fid", "flist").distinct()
val table2 = inputRDD.toDF("uid", "fid", "flist").distinct()
val table3 = table1.join(table2, table1("uid") <=> table2("fid") && table1("fid") <=> table2("uid")).select(table1("uid"), table1("fid"), table1("flist").as("flist1"), table2("flist").as("flist2"))

val mfcount = udf { 
	(Set1: WrappedArray[String], Set2: WrappedArray[String]) => (Set1.toList.intersect(Set2.toList)).size
}

val table4 = table3.withColumn("mfcount", mfcount(col("flist1"), col("flist2"))).select("uid", "fid", "mfcount").orderBy($"mfcount".desc).limit(10)
var table5 = table4.select("*")

val userFile = sc.textFile("userdata.txt")
val userRDD = userFile.map(line => line.split(",")).map(line => (line(0), line(1), line(2), line(3)))

val userTable = userRDD.toDF("userid", "fname", "lname", "address")
val joinTable1 = table5.join(userTable, table5("uid") <=> userTable("userid"))
val joinTable2 = userTable.join(joinTable1, joinTable1("fid") <=> userTable("userid"))

val output = joinTable2.select(joinTable1("mfcount"), joinTable1("fname"), joinTable1("lname"), joinTable1("address"), userTable("fname"), userTable("lname"), userTable("address"))

output.rdd.repartition(1).saveAsTextFile("question2_sql.out")
