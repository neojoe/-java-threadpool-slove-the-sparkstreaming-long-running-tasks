package com.surfilter.Utils

import java.util

import org.apache.hadoop.hbase.client.{Connection, _}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Athor： zhouning
  * Description：hbase的工具类
  * Created by on： 2017/8/12
  */
private[surfilter] class HbaseUtils(zkUrl:String) {
  val logger = LoggerFactory.getLogger("HbaseUtils")
  private val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", zkUrl)
  private val connection = ConnectionFactory.createConnection(hbaseConf)
  val scan = new Scan()
  val indentityTableName = TableName.valueOf("indentityrelation")
  val trackTableName = TableName.valueOf("indentitytrack")
  val indextableName = TableName.valueOf("indentityindex")

  def getHbaseConn: Connection = connection
  def getIndentityTable(conn:Connection):Table = conn.getTable(indentityTableName)
  def getTrackTable(conn:Connection):Table = conn.getTable(trackTableName)
  def getIndexTable(conn:Connection):Table = conn.getTable(indextableName)
  def getId:String={
    val ran = new Random()
    val sb = new StringBuffer()
    while(sb.length()<8) {
      val rand = ran.nextInt()&0x7FFFFFFF
      sb.append(Integer.toString(rand, 16))
    }
    val rn = ran.nextInt(9000)+1000
    val timeStamp = System.currentTimeMillis()
     sb.substring(0,8)+rn+timeStamp
  }
  def qeuryIndentityData(data: (String, String,String,String,String, String,String,String,String ,String, String,String, String, String, String,String, String),connection: Connection): Result = {

    var flag = 1
    var rowKey = ""
    val indexTable = getIndexTable(connection)
    val indentityTable = getIndentityTable(connection)


    if (flag == 1 && data._1!=null&&data._1.trim!="") {
      val get = new Get(Bytes.toBytes(data._1))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 2
        indexTable.close()

      }

    }

    if (flag == 2 &&data._2!=null&&data._2.trim!="") {

      val get = new Get(Bytes.toBytes(data._2))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 3
        indexTable.close()

      }

    }

    if (flag == 3 && data._3!=null && data._3.trim!="") {

      val get = new Get(Bytes.toBytes(data._3))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 4
        indexTable.close()

      }
    }
    if (flag == 4 &&data._4!=null &&data._4.trim!="") {
      val get = new Get(Bytes.toBytes(data._4))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 5
        indexTable.close()

      }
    }

    if (flag == 5 &&data._5!=null&&data._5.trim!="") {

      val get = new Get(Bytes.toBytes(data._5))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 6
        indexTable.close()

      }
    }

    if (flag == 6 && data._6!=null &&data._6.trim!="") {

      val get = new Get(Bytes.toBytes(data._6))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 1
        indexTable.close()

      }
    }
     if(rowKey!=null&&rowKey.trim!=""){

       val get = new Get(Bytes.toBytes(rowKey))
       indentityTable.get(get)
     /*  scan.setStartRow(Bytes.toBytes(rowKey))
       scan.setStopRow(Bytes.toBytes(rowKey+1))
       indentityTable.getScanner(scan)*/

     }else{

        null
     }
  }
  def qeuryIndentityId(inPut: String,connection:Connection):String={

    var rowKey = ""
    val indexTable = getIndexTable(connection)
    val indentityTable = getIndentityTable(connection)


    if (inPut.trim!="") {
      val get = new Get(Bytes.toBytes(inPut))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }

    }


    if(rowKey!=null&&rowKey.trim!=""){

      rowKey

    }else{

      null
    }
  }
  def qeuryMutlsIndentityId(data: (String, String,String,String,String, String,String,String,String ,String, String,String, String, String, String,String, String),connection: Connection): String = {

    var flag = 1
    var rowKey = ""
    val indexTable = getIndexTable(connection)
    val indentityTable = getIndentityTable(connection)


    if (flag == 1 && data._1!=null&&data._1.trim!="") {
      val get = new Get(Bytes.toBytes(data._1))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 2
        indexTable.close()

      }

    }

    if (flag == 2 &&data._2!=null&&data._2.trim!="") {

      val get = new Get(Bytes.toBytes(data._2))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 3
        indexTable.close()

      }

    }

    if (flag == 3 && data._3!=null && data._3.trim!="") {

      val get = new Get(Bytes.toBytes(data._3))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 4
        indexTable.close()

      }
    }
    if (flag == 4 &&data._4!=null &&data._4.trim!="") {
      val get = new Get(Bytes.toBytes(data._4))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 5
        indexTable.close()

      }
    }

    if (flag == 5 &&data._5!=null&&data._5.trim!="") {

      val get = new Get(Bytes.toBytes(data._5))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 6
        indexTable.close()

      }
    }

    if (flag == 6 && data._6!=null &&data._6.trim!="") {

      val get = new Get(Bytes.toBytes(data._6))
      val result = indexTable.get(get)

      if(result !=null && !result.isEmpty){

        rowKey =Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("indentityrowkey")))
        indexTable.close()

      }else {
        flag = 1
        indexTable.close()

      }
    }
    if(rowKey!=null&&rowKey.trim!=""){

      rowKey

    }else{

     null
    }
  }



}
