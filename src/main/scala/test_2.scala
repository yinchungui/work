import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

object test_2 {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.INFO)
    var masterUrl="local[2]"
    var appName="novie analysis"
    if(args.length>0){
      masterUrl=args(0)
    }else if(args.length>1){
      appName=args(1)
    }

    //创建上下问
    val conf=new SparkConf().setAppName(appName).setMaster(masterUrl);
    val sc=new SparkContext(conf);
    val filepath="data/"
    val usersRDD=sc.textFile(filepath+"users.dat")
    val occupationsRDD=sc.textFile(filepath+"occupations.dat")
    val ratingsRDD=sc.textFile(filepath+"ratings.dat")
    val moviesRDD=sc.textFile(filepath+"movies.dat")

//    occupationsRDD.cache()
//    usersRDD.cache()
//    ratingsRDD.cache()
//    moviesRDD.cache()
//
//    println("职业数："+occupationsRDD.count())
//    println("电影数："+moviesRDD.count())
//    println("用户数："+usersRDD.count())

    //某个用户看过的电影数量
    val userId="18"
    val userWatchedMovie=ratingsRDD.map(_.split("::"))
      .map(item => {(item(1),item(0))})
      .filter(_._2.equals(userId))
    println(userId+"观看过的电影数："+userWatchedMovie.count())

    println("这些电影的详情：\n")
    val movieInfoRDD=moviesRDD.map(_.split("::"))
      .map(movie => {(movie(0),(movie(1),movie(2)))})

    val result=userWatchedMovie.join(movieInfoRDD)
      .map(item=> {(item._1,item._2._1,item._2._2._1,item._2._2._2)})

    result.foreach(println)
    sc.stop()

  }
}
