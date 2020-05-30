import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

object Movie_1 {
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

    occupationsRDD.cache()
    usersRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()

    println("职业数："+occupationsRDD.count())
    println("电影数："+moviesRDD.count())
    println("用户数："+usersRDD.count())

    //需求：显示每个职业下用户信息
    val usersBasic=usersRDD.map(_.split("::"))//UserID,Gender,Age,OccupationID,Zip-code
      .map(user =>(user(3),(user(0),user(1),user(2),user(4)))//(OcupationID,(UserID,Gender,Age,Zip-code
    )
    val occupations=occupationsRDD.map(_.split("::"))
      .map(occupation =>{
        (occupation(0),occupation(1))
      })

    //合并
    val usersInfo=usersBasic.join(occupations)
    println("用户详情（格式：（职业编号，（人的编号，性别，年龄，邮编)，职业名)):")
    usersInfo.foreach(println)

    println("合并后共有："+usersInfo.count()+"条users记录")
    sc.stop()

  }
}
