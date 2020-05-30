import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

object test_5 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.INFO)
    var masterUrl = "local[2]"
    var appName = "novie analysis"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      appName = args(1)
    }

    //创建上下问
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
    val sc = new SparkContext(conf);
    val filepath = "data/"
    val usersRDD = sc.textFile(filepath + "users.dat")
    val occupationsRDD = sc.textFile(filepath + "occupations.dat")
    val ratingsRDD = sc.textFile(filepath + "ratings.dat")
    val moviesRDD = sc.textFile(filepath + "movies.dat")

    var age="1"
    println("年龄段为："+age +"喜爱的电影top 10:")
    val userWithinAge=usersRDD.map(_.split("::"))
      .map((x=>(x(0),x(2))))
      .filter(_._2.equals(age))//(UserId,age)

    sc.stop()
  }
}
