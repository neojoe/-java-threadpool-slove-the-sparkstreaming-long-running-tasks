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
  * Description: test git
  * Created by on: 2017/9/11
  */
object PortraitsAndTracks {
  private val logger = LoggerFactory.getLogger("ConstructPortraits")

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("ConstructPortraits")





    val borkerList = conf.get("spark.kafka.broker.list")
    val zkUrl = conf.get("spark.zookeeper.url")
    val topics = conf.get("spark.kafka.topics")
    val groupId =conf.get("spark.zookeeper.groupid")
    val batchs =conf.getInt("spark.hbase.batchs",500)
    val interval =conf.getInt("spark.batchs.interval",20)
    val checkpointDir =conf.get("spark.checkpoint.dir")
    val partitions =conf.getInt("spark.repartition.num",20)
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

          val indentityList = new util.ArrayList[Put]()
          val trackList = new util.ArrayList[Put]()
          val trackTable = hb.getTrackTable(connetion)
          val indentityTable = hb.getIndentityTable(connetion)

          if (partition != null) {

            partition.foreach(pairs => {

              val indentityTablename = "indentityrelation"
              val trackTablename = "indentitytrack"
              val indentityFamily = "cf"
              val trackFamily = "cf"


              var newIndentityList = List[String]()


              if (!newIndentityList.contains(pairs._1) && pairs._1 != "" && pairs._1.trim != "") {
                newIndentityList = pairs._1 :: newIndentityList
              }
              if (!newIndentityList.contains(pairs._2) && pairs._2 != "" && pairs._2.trim != "") {
                newIndentityList = pairs._2 :: newIndentityList
              }
              if (!newIndentityList.contains(pairs._3) && pairs._3 != "" && pairs._3.trim != "") {
                newIndentityList = pairs._3 :: newIndentityList
              }
              if (!newIndentityList.contains(pairs._4) && pairs._4 != "" && pairs._4.trim != "") {
                newIndentityList = pairs._4 :: newIndentityList
              }
              if (!newIndentityList.contains(pairs._5) && pairs._5 != "" && pairs._5.trim != "") {
                newIndentityList = pairs._5 :: newIndentityList
              }
              if (!newIndentityList.contains(pairs._6) && pairs._6 != "" && pairs._6.trim != "") {
                newIndentityList = pairs._6 :: newIndentityList
              }

              val indentityCount = newIndentityList.length + ""
              val newIndentityListStr = newIndentityList.mkString(" ")

              val rs = hb.qeuryIndentityData(pairs, connetion)


              if (rs != null&& !rs.isEmpty) {

                val indentityRowKey = Bytes.toString(rs.getRow)
                val indentity = Bytes.toString(rs.getValue(Bytes.toBytes(indentityFamily), Bytes.toBytes("indentity")))

                var mergeIndentityList = List[String]()
                if (indentity != null && indentity.trim != "") {

                  val indentitySplit = indentity.split(" ")
                  if (indentitySplit.length <= 30) {
                    for (element <- indentitySplit) {
                      mergeIndentityList = element :: mergeIndentityList
                    }

                    if (!mergeIndentityList.contains(pairs._1) && pairs._1 != "") {
                      mergeIndentityList = pairs._1 :: mergeIndentityList
                    }
                    if (!mergeIndentityList.contains(pairs._2) && pairs._2 != "") {
                      mergeIndentityList = pairs._2 :: mergeIndentityList
                    }
                    if (!mergeIndentityList.contains(pairs._3) && pairs._3 != "") {
                      mergeIndentityList = pairs._3 :: mergeIndentityList
                    }
                    if (!mergeIndentityList.contains(pairs._4) && pairs._4 != "") {
                      mergeIndentityList = pairs._4 :: mergeIndentityList
                    }
                    if (!mergeIndentityList.contains(pairs._5) && pairs._5 != "") {
                      mergeIndentityList = pairs._5 :: mergeIndentityList
                    }
                    if (!mergeIndentityList.contains(pairs._6) && pairs._6 != "") {
                      mergeIndentityList = pairs._6 :: mergeIndentityList
                    }

                    val indentityCount = mergeIndentityList.length + ""
                    val mergeIndentityStr = mergeIndentityList.mkString(" ")




                    try {

                      val indentityPut = new Put(Bytes.toBytes(indentityRowKey))

                      indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("indentity"), Bytes.toBytes(mergeIndentityStr))
                      indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("indentitytimes"), Bytes.toBytes(indentityCount))

                      if (pairs != null && pairs._14 != "" && pairs._14 != "MULL") {
                        indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("age"), Bytes.toBytes(pairs._14))
                      }
                      if (pairs != null && pairs._15 != "" && pairs._15 != "MULL") {
                        indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("sex"), Bytes.toBytes(pairs._15))
                      }
                      /*   if (pairs != null && pairs._16 != "" && pairs._16 != "MULL") {
                           indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("name"), Bytes.toBytes(pairs._16))
                         }*/
                      if (pairs != null && pairs._17 != "" && pairs._17 != "MULL") {
                        indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("brand"), Bytes.toBytes(pairs._17))

                      }

                      indentityList.add(indentityPut)

                      if (indentityList.size() >= batchs) {

                        indentityTable.put(indentityList)
                        indentityList.clear()

                      }

                      logger.info(s">>>>>>>>>>>>>>insert indentity record indentity success ! ")
                    } catch {
                      case ex: Exception => logger.warn(s">>>>>>>>>>>>>>insert indentity record fail ! ", ex)
                    }


                    try {
                      //   val trackRowKey = UUID.randomUUID().toString.replaceAll("-", "")
                      if(pairs._7!=null&&pairs._7!="MULL"&&pairs._9!=null&&pairs._9!="MULL") {
                        val trackRowKey = hb.getId
                        val trackPut = new Put(Bytes.toBytes(trackRowKey))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("indentityid"), Bytes.toBytes(indentityRowKey))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("servicecode"), Bytes.toBytes(pairs._7))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("servicecodetype"), Bytes.toBytes(pairs._8))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("starttime"), Bytes.toBytes(pairs._9))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("endtime"), Bytes.toBytes(pairs._10))
                        //  trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("source"), Bytes.toBytes(pairs._11))
                        trackPut.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("datatype"), Bytes.toBytes(pairs._13))

                        trackList.add(trackPut)
                      }
                      if (trackList.size() >= batchs) {

                        trackTable.put(trackList)
                        trackList.clear()

                      }
                      logger.info(s">>>>>>>>>>>>>>>insert record  success ! ")
                    } catch {
                      case ex: Exception => logger.warn(s">>>>>>>>>>>>>>>insert record  fail ! ")
                    }


                  } else {
                    try {
                      val del = new Delete(Bytes.toBytes(indentityRowKey))
                      val indentityTable = hb.getIndentityTable(connetion)

                      indentityTable.delete(del)
                      indentityTable.close()

                      logger.info(s">>>>>>>>>>>>>>>close table of hbase success ! ")
                    } catch {
                      case ex: Exception => logger.warn(s">>>>>>>>>>>>>>>close table of hbase fail ! ", ex)
                    }


                  }
                }
              }

            })

            if (indentityList.size() > 0) {

              indentityTable.put(indentityList)
              indentityList.clear()
              if(indentityTable!=null) {
                indentityTable.close()
              }
            }
            if (trackList.size() > 0) {

              trackTable.put(trackList)
              trackList.clear()
              if(trackTable!=null){
                trackTable.close()
              }
            }
          }

          if(connetion!=null){
            connetion.close()
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
