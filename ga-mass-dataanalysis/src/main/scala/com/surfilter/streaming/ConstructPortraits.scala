package com.surfilter.streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{Callable, Executors, Future}

import com.surfilter.Utils.HbaseUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{Delete, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Athor: zhouning
  * Description: 
  * Created by on: 2017/9/11
  */
object ConstructPortraits {
  private val logger = LoggerFactory.getLogger("ConstructPortraits")

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("ConstructPortraits")





    val borkerList = conf.get("spark.kafka.broker.list")
    val zkUrl = conf.get("spark.zookeeper.url")
    val topics = conf.get("spark.kafka.topics")
    val groupId =conf.get("spark.zookeeper.groupid")
    val batchs =conf.getInt("spark.hbase.batchs",500)
    val interval =conf.getInt("spark.batchs.interval",3)
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
        val len = parts.length
        val result = len match {
          //IM
          case 38 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(4)&&"MULL" == parts(1)) {
              indentityOne = ""
            } else {
              indentityOne = parts(4) + "|"+parts(1)
            }
            if ("MULL" == parts(15)||parts(16)=="ode=1"||parts(16)=="MULL") {
              indentityTwo = ""
            } else {


              if (parts(15) != null && parts(15) .matches("^[0-9]{17}[0-9X]$") && parts(15) != "" && parts(15).toInt == 1021111 && parts(16) != null &&parts(16) .matches("^\\d+$")&& parts(16).length == 18) {
                val birthday = parts(16).substring(6, 10).toInt
                val sexNum = parts(16).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityTwo = parts(16) + "|1021111"
              }else if(parts(15) .matches("^\\d+$")){
                indentityTwo = parts(16) + "|" + parts(15)
              }

            }

            if ("MULL" == parts(30)||"MULL" == parts(29)) {
              indentityThree = ""
            } else {


              if (parts(29) != null && parts(29) .matches("^[0-9]{17}[0-9X]$") && parts(29).toInt == 1021000 && parts(30) != null&&parts(30) .matches("^\\d+$") && parts(30).length == 18) {
                val birthday = parts(30).substring(6, 10).toInt
                val sexNum = parts(30).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityThree = parts(30) + "|1021111"
              } else if(parts(29) .matches("^\\d+$")) {
                indentityThree = parts(30) + "|" + parts(29)
              }
              if ("MULL" == parts(21)) {
                indentityOne = ""
              } else {
                indentityOne = parts(21) + "|1020004"
              }

            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", parts(2), parts(3), parts(8), "", parts(34), parts(12), "1", age, sex,parts(14),"")
          }
          //wl
          case 36  => {
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
            (indentityOne, indentityTwo, indentityThree, indentityFour, indentityFive, "", parts(18), parts(28), parts(2), parts(3), parts(33), "", "2", "", "","",parts(8))
          }

          //xw
          case 46 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(14) || "MULL" == parts(15)||parts(15)=="ode=1") {
              indentityOne = ""
            } else {


              if (parts(14) != null && parts(14) .matches("^[0-9]{17}[0-9X]$") && parts(14).toInt == 1021111 && parts(15) != null&& parts(15) .matches("^\\d+$")&& parts(15).length == 18) {
                val birthday = parts(15).substring(6, 10).toInt
                val sexNum = parts(15).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                  indentityOne = parts(15) + "|1021111"
                }
              }else if(parts(14) .matches("^\\d+$")){
                indentityOne = parts(15) + "|" + parts(14)
              }
            }
            if ("MULL" == parts(16)) {
              indentityTwo = ""

            } else {
              indentityTwo = parts(16) + "|1020004"
            }

            if ("MULL" == parts(39)||"MULL" == parts(38) ) {
              indentityThree = ""
            } else {


              if (parts(38) != null&& parts(38) .matches("^[0-9]{17}[0-9X]$")  && parts(38).toInt == 1021000 && parts(39) != null&&parts(39) .matches("^\\d+$") && parts(39).length == 18) {
                val birthday = parts(39).substring(6, 10).toInt
                val sexNum = parts(39).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityThree = parts(39) + "|1021111"
              } else if(parts(38) .matches("^\\d+$")) {
                indentityThree = parts(39) + "|" + parts(38)
              }

            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", parts(0), parts(32), parts(3), "", parts(41), "", "3", age, sex,parts(13),"")
          }
          //sj
          case 51 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(2) || "MULL" == parts(3)||parts(3)=="ode=1") {
              indentityTwo = ""
            } else {

              if (parts(2) != null && parts(2) .matches("^[0-9]{17}[0-9X]$") && parts(2).toInt == 1021111 && parts(3) != null&&parts(3) .matches("^\\d+$") && parts(3).length == 18) {
                val birthday = parts(3).substring(6, 10).toInt
                val sexNum = parts(3).charAt(16).toInt
                val now = new Date()
                val formator = new SimpleDateFormat("yyyy")
                val today = formator.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityTwo = parts(3) + "|1021111"
              }else if(parts(2) .matches("^\\d+$")){
                indentityTwo = parts(3) + "|" + parts(2)
              }

            }
            if ("MULL" == parts(13)|| !parts(13).matches("^\\d+$")) {
              indentityThree = ""

            } else {
              indentityThree = parts(13) + "|1020004"
            }

            if ( "MULL" == parts(28)||"MULL" == parts(27) ) {
              indentityFour = ""
            } else {

              if (parts(27) != null && parts(27) .matches("^[0-9]{17}[0-9X]$") && parts(27).toInt == 1021000 && parts(28) != null&&parts(28) .matches("^\\d+$") && parts(28).length == 18) {
                val birthday = parts(28).substring(6, 10).toInt
                val sexNum = parts(28).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityFour = parts(28) + "|1021111"
              } else if(parts(27) .matches("^\\d+$")){
                indentityFour = parts(28) + "|" + parts(27)
              }
            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", parts(0), parts(32), parts(4), parts(5), parts(45), "", "4", age, sex,parts(1),parts(44))
          }
          //fj
          case 52 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var indentityFive = ""
            var indentitySix = ""
            var age = ""
            var sex = ""
            if ("MULL" == parts(34)|| parts(34)=="ode=1" || parts(35) == "MULL") {
              indentityOne = ""
            } else {


              if (parts(34) != null && parts(34) .matches("^[0-9]{17}[0-9X]$") && parts(34).length == 18 && parts(35) != null&&parts(35) .matches("^\\d+$") && parts(35).toInt == 1021111) {
                val birthday = parts(34).substring(6, 10).toInt
                val sexNum = parts(34).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityOne = parts(34) + "|1021111"
              }else if(parts(35) .matches("^\\d+$")){
                indentityOne = parts(34) + "|" + parts(35)
              }
            }

            if ("MULL" == parts(32)|| !parts(32).matches("^\\d+$")) {
              indentityTwo = ""
            } else {
              indentityTwo = parts(32) + "|1020004"
            }



            if ("MULL" == parts(36)) {
              indentityFour = ""

            } else {
              indentityFour = parts(36) + "|1021901"
            }

            if ("MULL" == parts(37)) {
              indentityFive = ""
            } else {
              indentityFive = parts(37) + "|1020003"
            }

            if ("MULL" == parts(43)||"MULL" == parts(42) ) {
              indentitySix = ""
            } else {


              if (parts(42) != null && parts(42) .matches("^[0-9]{17}[0-9X]$") && parts(42).toInt == 1021000 && parts(43) != null&&parts(43) .matches("^\\d+$") && parts(43).length == 18) {
                val birthday = parts(43).substring(6, 10).toInt
                val sexNum = parts(43).charAt(12).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0" //女
                } else {
                  sex = "1" //男
                }
                indentitySix = parts(43) + "|1021111"
              } else if(parts(42) .matches("^\\d+$")){
                indentitySix = parts(43) + "|" + parts(42)
              }

            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, indentityFive, indentitySix, parts(2), parts(3), parts(4), parts(5), parts(47), "", "5", age, sex,parts(33),"")
          }
          //  ct
          case 42 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var indentityFive = ""
            var indentitySix = ""
            var age = ""
            var sex = ""
            if ("MULL" == parts(25) || parts(26)=="ode=1"||parts(26) == "MULL") {
              indentityOne = ""
            } else {


              if (parts(26) != null&& parts(26) .matches("^[0-9]{17}[0-9X]$")  && parts(26).length == 18 && parts(25) != null&&parts(25) .matches("^\\d+$") && parts(25).toInt == 1021111) {
                val birthday = parts(26).substring(6, 10).toInt
                val sexNum = parts(26).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityOne = parts(26) + "|1021111"
              }else if(parts(25) .matches("^\\d+$")){
                indentityOne = parts(26) + "|" + parts(25)
              }
            }

            if ("MULL" == parts(27)|| !parts(27).matches("^\\d+$")) {
              indentityTwo = ""
            } else {
              indentityTwo = parts(27) + "|1020004"
            }




            if ("MULL" == parts(34)|| "MULL" == parts(33)  ) {
              indentityThree = ""
            } else {


              if (parts(33) != null && parts(33) .matches("^[0-9]{17}[0-9X]$") && parts(33).toInt == 1021000 && parts(34) != null&&parts(34) .matches("^\\d+$") && parts(34).length == 18) {
                val birthday = parts(34).substring(6, 10).toInt
                val sexNum = parts(34).charAt(12).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityThree = parts(34) + "|1021111"
              } else if(parts(33) .matches("^\\d+$")){
                indentityThree = parts(34) + "|" + parts(33)
              }

            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, "", "", "", parts(1), "", parts(12),"", parts(38), "", "6", age, sex,parts(24),"")
          }
          //tz
          case 28 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var indentityFive = ""
            var indentitySix = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(2)|| !parts(2).matches("^\\d+$")) {
              indentityTwo = ""
            } else {
              indentityTwo = parts(2) + "|1020004"
            }
            if ("MULL" == parts(6)) {
              indentityOne = ""
            } else {
              indentityOne = parts(6) + "|1020003"
            }
            if ("MULL" == parts(7)) {
              indentityThree = ""
            } else {
              indentityThree = parts(7) + "|1021901"
            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, "", "", "", parts(20), parts(26), parts(0), "", parts(27), "", "7", age, sex,"","")
          }
          //kw
          case 40 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(31)|| !parts(31).matches("^\\d+$")) {
              indentityOne = ""
            } else {
              indentityOne = parts(31) + "|1020004"
            }

            if ("MULL" == parts(30)||parts(30)=="ode=1") {
              indentityTwo = ""
            } else {


              if (parts(29) != null && parts(29) .matches("^[0-9]{17}[0-9X]$") && parts(29) != "" && parts(29).toInt == 1021111 && parts(30) != null&&parts(30) .matches("^\\d+$") && parts(30).length == 18) {
                val birthday = parts(30).substring(6, 10).toInt
                val sexNum = parts(30).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityTwo = parts(30) + "|1021111"
              }else if (parts(29) .matches("^\\d+$")){
                indentityTwo = parts(30) + "|" + parts(29)
              }

            }

            if ("MULL" == parts(33)||"MULL" == parts(32)) {
              indentityThree = ""
            } else {


              if (parts(32) != null && parts(32) .matches("^[0-9]{17}[0-9X]$") && parts(32).toInt == 1021000 && parts(33) != null&&parts(33) .matches("^\\d+$") && parts(33).length == 18) {
                val birthday = parts(33).substring(6, 10).toInt
                val sexNum = parts(33).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityThree = parts(33) + "|1021111"
              } else if(parts(32) .matches("^\\d+$")){
                indentityThree = parts(33) + "|" + parts(32)
              }

            }
            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", parts(1), "", parts(2), "","", parts(16), "8", age, sex,parts(28),"")
          }
          //ka
          case 21 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(18)|| !parts(18).matches("^\\d+$")) {
              indentityOne = ""
            } else {
              indentityOne = parts(18) + "|1020004"
            }

            if ("MULL" == parts(8)||parts(8)=="ode=1") {
              indentityTwo = ""
            } else {


              if (parts(7) != null && parts(7) .matches("^[0-9]{17}[0-9X]$") && parts(7) != "" && parts(7).toInt == 1021111 && parts(8) != null&&parts(8) .matches("^\\d+$") && parts(8).length == 18) {
                val birthday = parts(8).substring(6, 10).toInt
                val sexNum = parts(8).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityTwo = parts(8) + "|1021111"
              }else if(parts(7) .matches("^\\d+$")){
                indentityTwo = parts(8) + "|" + parts(7)
              }

            }



            //场所编号 场所类型 start_time 6	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", parts(0), "", parts(12), "",parts(19), "", "9", age, sex,parts(1),"")
          }
          //rz
          case 17 => {
            var indentityOne = ""
            var indentityTwo = ""
            var indentityThree = ""
            var indentityFour = ""
            var indentityFive = ""
            var age = ""
            var sex = ""

            if ("MULL" == parts(0)) {
              indentityOne = ""
            } else {
              indentityOne = parts(0) + "|1020002"
            }

            if ("MULL" == parts(7)||parts(7)=="ode=1") {
              indentityTwo = ""
            } else {


              if (parts(6) != null && parts(6) .matches("^[0-9]{17}[0-9X]$") && parts(6) != "" && parts(6).toInt == 1021111 && parts(7) != null &&parts(7) .matches("^\\d+$")&& parts(7).length == 18) {
                val birthday = parts(7).substring(6, 10).toInt
                val sexNum = parts(7).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityTwo = parts(7) + "|1021111"
              }else if(parts(6) .matches("^\\d+$")){
                indentityTwo = parts(7) + "|" + parts(6)
              }
            }
            if ("MULL" == parts(3)||"MULL" == parts(2)) {
              indentityThree = ""
            } else {


              if (parts(2) != null&& parts(2) .matches("^[0-9]{17}[0-9X]$")  && parts(2).toInt == 1021000 && parts(3) != null &&parts(3) .matches("^\\d+$")&& parts(3).length == 18) {
                val birthday = parts(3).substring(6, 10).toInt
                val sexNum = parts(3).charAt(16).toInt
                val now = new Date()
                val format = new SimpleDateFormat("yyyy")
                val today = format.format(now).toInt
                val gap = today - birthday

                age = gap + ""
                if (sexNum % 2 == 0) {
                  sex = "0"
                } else {
                  sex = "1"
                }
                indentityThree = parts(3) + "|1021111"
              } else if(parts(2) .matches("^\\d+$")) {
                indentityThree = parts(3) + "|" + parts(2)
              }

            }
            if ("MULL" == parts(14)) {
              indentityFour = ""
            } else {
              indentityFour = parts(14) + "|1021901"
            }
            if ("MULL" == parts(15)) {
              indentityFive = ""
            } else {
              indentityFive = parts(15) + "|1020003"
            }

            //场所编号 场所类型 start_time 	end_time sysType
            (indentityOne, indentityTwo, indentityThree, indentityFour, "", "", "", "", parts(6), "",parts(5), "", "10", age, sex,parts(8),"")
          }

          case _ => ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "","","")
        }

        resultList = result :: resultList
      }
      resultList.iterator
    })

    extractData.foreachRDD(rdd => {

      if(rdd!=null) {
        rdd.foreachPartition(partition => {

          //val zkUrl = zkUrlBroadcast.value.toString
          val hb = new HbaseUtils(zkUrl)
          val connetion = hb.getHbaseConn
          val indexList = new util.ArrayList[Put]()
          val indentityList = new util.ArrayList[Put]()
          val trackList = new util.ArrayList[Put]()
          val trackTable = hb.getTrackTable(connetion)
          val indentityTable = hb.getIndentityTable(connetion)
          val indexTable = hb.getIndexTable(connetion)

          var resultList = List[Future[Int]]()
          val pool = Executors.newFixedThreadPool(20)
          class RelationToHbase(dataList:util.ArrayList[Put]) extends Callable[Int]{
            override def call(): Int = {

              try {
                indentityTable.put(dataList)
                dataList.clear()
                indexTable.close()
              } catch {
                case e:Exception => logger.error("<<<<<<<<<<<<<<<insert data fail")
              }
              1
            }
          }
          class TrackToHbase(dataList:util.ArrayList[Put]) extends Callable[Int]{
            override def call(): Int = {
              try {
                trackTable.put(dataList)
                dataList.clear()
                trackTable.close()
              } catch {
                case e:Exception => logger.error("<<<<<<<<<<<<<<<insert data fail")
              }
              1
            }
          }
          class IndexToHbase(dataList:util.ArrayList[Put]) extends Callable[Int]{
            override def call(): Int = {
              try {
                indexTable.put(dataList)
                dataList.clear()
                indexTable.close()
              } catch {
                case e:Exception => logger.error("<<<<<<<<<<<<<<<insert data fail")
              }
              1
            }
          }
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


                    if (pairs._1 != null && pairs._1.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._1))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }

                    if (pairs._2 != null && pairs._2.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._2))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }

                    if (pairs._3 != null && pairs._3.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._3))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }
                    if (pairs._4 != null && pairs._4.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._4))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }

                    if (pairs._5 != null && pairs._5.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._5))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }

                    if (pairs._6 != null && pairs._6.trim != "") {
                      val put = new Put(Bytes.toBytes(pairs._6))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("indentityrowkey"), Bytes.toBytes(indentityRowKey))
                      indexList.add(put)
                    }

                    if (indexList.size() >= batchs) {

                      val copy = new util.ArrayList[Put]()
                      copy.addAll(indexList)
                      val result = pool.submit(new IndexToHbase(copy))
                      resultList::= result
                      indexList.clear()

                    }


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
                        val copy = new util.ArrayList[Put]()
                        copy.addAll(indentityList)
                        val result =  pool.submit(new RelationToHbase(copy))
                        resultList::= result
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

                        val copy = new util.ArrayList[Put]()
                        copy.addAll(trackList)
                        val result = pool.submit(new TrackToHbase(copy))
                        resultList::= result
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
              } else if ((rs == null||rs.isEmpty)&& newIndentityListStr != null && newIndentityListStr.trim != "") {

                val indentityRowKey = hb.getId
                // val indentityRowKey = UUID.randomUUID().toString().replaceAll("-", "")

                try {

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

                  val indentityPut = new Put(Bytes.toBytes(indentityRowKey))
                  indentityPut.addColumn(Bytes.toBytes(indentityFamily), Bytes.toBytes("indentity"), Bytes.toBytes(newIndentityListStr))
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

                  if (indexList.size() >= batchs) {

                    val copy = new util.ArrayList[Put]()
                    copy.addAll(indexList)
                    val result = pool.submit(new IndexToHbase(copy))
                    resultList::= result
                    indexList.clear()

                  }
                  if (indentityList.size() >= batchs) {

                    val copy = new util.ArrayList[Put]()
                    copy.addAll(indentityList)
                    val result = pool.submit(new RelationToHbase(copy))
                    resultList::= result
                    indentityList.clear()

                  }
                  logger.info(s">>>>>>>>>>>>>>>insert record  success ! ")
                } catch {
                  case ex: Exception => logger.warn(s">>>>>>>>>>>>>>>insert record  fail ! ")
                }



                try {
                  // val trackRowKey = UUID.randomUUID().toString().replaceAll("-", "")
                  if(pairs._7!=null&&pairs._7!="MULL"&&pairs._9!="MULL"&&pairs._9!="MULL") {
                    val trackRowKey = hb.getId
                    val putTrack = new Put(Bytes.toBytes(trackRowKey))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("indentityid"), Bytes.toBytes(indentityRowKey))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("servicecode"), Bytes.toBytes(pairs._7))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("servicecodetype"), Bytes.toBytes(pairs._8))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("starttime"), Bytes.toBytes(pairs._9))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("endtime"), Bytes.toBytes(pairs._10))
                    // putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("source"), Bytes.toBytes(pairs._11))
                    putTrack.addColumn(Bytes.toBytes(trackFamily), Bytes.toBytes("datatype"), Bytes.toBytes(pairs._13))
                    trackList.add(putTrack)
                  }

                  if (trackList.size() >= batchs) {

                    val copy = new util.ArrayList[Put]()
                    copy.addAll(trackList)
                    val result =  pool.submit(new TrackToHbase(copy))
                    resultList::= result
                    trackList.clear()

                  }
                  logger.info(">>>>>>>>>>>>>>>insert record  success ! ")
                } catch {
                  case ex: Exception => logger.warn(">>>>>>>>>>>>>>>insert record  fail ! ")
                }

              }

            })
            if (indexList.size() > 0) {

              val copy = new util.ArrayList[Put]()
              copy.addAll(indexList)
              val result = pool.submit(new IndexToHbase(copy))
              resultList::= result
              indexList.clear()

            }
            if (indentityList.size() > 0) {
              val copy = new util.ArrayList[Put]()
              copy.addAll(indentityList)
              val result = pool.submit(new RelationToHbase(copy))
              resultList::= result
              indentityList
            }
            if (trackList.size() > 0) {
              val copy = new util.ArrayList[Put]()
              copy.addAll(trackList)
              val result = pool.submit(new TrackToHbase(copy))
              resultList::= result
              trackList.clear()
            }
          }
          pool.shutdown()
          if(!resultList.isEmpty){
            for(result <- resultList){

                  result.get()

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
