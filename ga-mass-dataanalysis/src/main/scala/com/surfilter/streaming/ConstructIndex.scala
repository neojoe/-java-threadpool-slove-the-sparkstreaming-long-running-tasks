package com.surfilter.streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.surfilter.Utils.HbaseUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

/**
  * Athor: zhouning
  * Description: gogogo
  * Created by on: 2017/9/11
  */
object ConstructIndex {
  private val logger = LoggerFactory.getLogger("ConstructPortraits")

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("ConstructPortraits")




    val borkerList = conf.get("spark.kafka.broker.list")
    val zkUrl = conf.get("spark.zookeeper.url")
    val topics = conf.get("spark.kafka.topics")
    val groupId =conf.get("spark.zookeeper.groupid")
    val checkpointDir =conf.get("spark.checkpoint.dir")
    val batchs =conf.getInt("spark.hbase.batchs",500)
    val partitions =conf.getInt("spark.repartition.num",20)

    val interval =conf.getInt("spark.batchs.interval",3)
    val ssc = new StreamingContext(conf, Seconds(interval))
    ssc.checkpoint(checkpointDir)
    val zkUrlBroadcast = ssc.sparkContext.broadcast(zkUrl)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> borkerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest"
    )

    val topicSet = topics.split(",").toSet

    var messages  =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).repartition(partitions)


    try {
    } catch {
      case ex:Exception => logger.warn("<<<<<<<<<<<<<<< read the data from kafka exception !")
    }

    val extractData = messages.mapPartitions(iter => {
      var resultList: List[(String, String, String, String, String,String,String ,String, String, String, String, String, String, String, String, String, String)] = List(("", "", "", "", "", "", "", "", "", "", "", "", "", "", "","",""))

      while (iter.hasNext) {
        val x = iter.next()
        val parts = x._2.split("\\|")

            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var indentityFive = ""

            if ("MULL" == parts(0)) {
              indentityOne = ""
            } else {
              indentityOne = parts(0) + "|1020002"
            }
            if ("MULL" == parts(10)) {
              indentityTwo = ""
            } else {
              indentityTwo = parts(10) + "|1021901"
            }
            if ("MULL" == parts(11)) {
              indentityThree = ""

            } else {
              indentityThree = parts(11) + "|1020003"
            }

            if ("MULL" == parts(24)|| !parts(24).matches("^\\d+$") || parts(24).length<5||"MULL" == parts(23)) {
              indentityFive = ""
            } else {
              indentityFive = parts(24) + "|"+parts(23)
            }
            //场所编号 场所类型 start_time 6	end_time sysType
          resultList =  (indentityOne, indentityTwo, indentityThree, indentityFour, indentityFive, "", parts(18), parts(28), parts(2), parts(3), parts(33), "", "2", "", "","",parts(8))::resultList

      }
      resultList.iterator
    })

    extractData.foreachRDD(rdd => {

      if(rdd!=null) {
        rdd.foreachPartition(partition => {
          val zkUrl = zkUrlBroadcast.value
          val hb = new HbaseUtils(zkUrl)
          val connetion = hb.getHbaseConn
          val indexList = new util.ArrayList[Put]()
          val indexTable = hb.getIndexTable(connetion)

          if (partition != null) {

            partition.foreach(pairs => {

              val indentityTablename = "indentityrelation"
              val trackTablename = "indentitytrack"
              val indentityFamily = "cf"
              val trackFamily = "cf"

              val rs = hb.qeuryMutlsIndentityId(pairs, connetion)


              if (rs != null && !rs.isEmpty) {


                if (pairs._1 != null && pairs._1.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._1))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }

                if (pairs._2 != null && pairs._2.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._2))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }

                if (pairs._3 != null && pairs._3.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._3))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }
                if (pairs._4 != null && pairs._4.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._4))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }

                if (pairs._5 != null && pairs._5.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._5))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }

                if (pairs._6 != null && pairs._6.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._6))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(rs))
                  indexList.add(put)
                }

                if (indexList.size() >= batchs) {

                  indexTable.put(indexList)
                  indexList.clear()

                }


              } else if (rs == null || rs.isEmpty) {

                val indentityRowKey = hb.getId


                if (pairs._1 != null && (!pairs._1.isEmpty) && pairs._1.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._1))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }

                if (pairs._2 != null && (!pairs._2.isEmpty) && pairs._2.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._2))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }

                if (pairs._3 != null && (!pairs._3.isEmpty) && pairs._3.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._3))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }
                if (pairs._4 != null && (!pairs._4.isEmpty) && pairs._4.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._4))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }

                if (pairs._5 != null && (!pairs._5.isEmpty) && pairs._5.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._5))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }

                if (pairs._6 != null && (!pairs._6.isEmpty) && pairs._6.trim != "") {
                  val put = new Put(Bytes.toBytes(pairs._6))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                  indexList.add(put)
                }


                if (indexList.size() >= batchs) {

                  indexTable.put(indexList)
                  indexList.clear()

                }
              }
            })
            if (indexList.size() > 0) {

              indexTable.put(indexList)
              indexList.clear()
              if (indexTable != null) {
                indexTable.close()
              }
            }

            if (connetion != null) {
              connetion.close()
            }
          }
        })
      }
    })
    ssc
  }

  def main(args: Array[String]): Unit = {

    val context = StreamingContext.getOrCreate(args(0), functionToCreateContext)
    context.start()
    context.awaitTermination()

  }
}
