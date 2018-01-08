package com.surfilter.Utils

import java.util
import java.util.Date
import java.util.concurrent.{Callable, Executors, Future}

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import org.elasticsearch.spark._


/**
  * Athor: zhouning
  * Description: 
  * Created by on: 2017/9/1
  */
object SparkTest {
  def hashId(input: String): String = Hashing.md5().hashString(input, Charsets.UTF_8).toString


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()



/*    var list = List[Future[Integer]]()
   // val data = hiveContext.sql("select mac,start_time from all_tracks_parquet limit 10")
    val pool = Executors.newFixedThreadPool(3)

    val result1 = pool.submit(new DataToHive(2))
    list::=result1
    val result2 = pool.submit(new DataToHBase(1000))
    list::=result2


    println("****************")
    pool.shutdown()
    println(s"*******${result1.get()}******")
    println(s"*******${result2.get()}******")*/
  }
/*
  class DataToEs(data:DataFrame,name:String) extends Callable[Integer]{
    override def call(): Integer = {
      val option1 = Map(
        "pushdown" -> "true",
        "number_of_shards" -> "6",
        "es.index.auto.create" -> "true",
        "number_of_replicas" -> "2",
        "es.nodes" -> "192.168.0.143",
        "es.port" -> "9200")
     1
      data.write.mode(SaveMode.Append).format("org.elasticsearch.spark.sql").options(option1).save("spark/test")
    }
  }
*/


  class DataToHive(in:Integer) extends Callable[Integer]{
    override def call(): Integer = {
      var sum = in
      for(i<-1 to 10){
        sum+=i
      }
       sum
    }
  }
  class DataToHBase(in:Integer) extends Callable[Integer]{
    override def call(): Integer = {
      var sum = in
      for(i<-1 to 10){
        Thread.sleep(100)
        sum+=i
      }
      sum
    }
  }
 /* class DataToHive(data:DataFrame,name:String) extends Callable[Integer]{
    override def call(): Integer = {
      data.write.mode(SaveMode.Append).saveAsTable(name)
      10
    }
  }*/

}
