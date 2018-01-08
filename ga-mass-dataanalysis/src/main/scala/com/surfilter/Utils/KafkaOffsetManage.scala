package com.surfilter.Utils

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

private[surfilter] object KafkaOffsetManage{
   private val logger = LoggerFactory.getLogger("KafkaOffsetUtils")

   def zk() = {

      val zkClientAndConnection = ZkUtils.createZkClientAndConnection("rzx168:2181,rzx169:2181,rzx177:2181", 3000, 3000)
      val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
      zkUtils
   }

   def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], topics: Seq[String]): InputDStream[(String, String)] = {

      var storedOffsets: Option[Map[TopicAndPartition, Long]] = None
      val groupId = kafkaParams.get("group.id").get
      try {
         storedOffsets = readOffsets(topics, groupId, zk())
      } catch {
         case ex: Exception =>
            System.exit(1)
      }

      val messages = storedOffsets match {
         case None =>
            try {
               logger.error("***********bbbbbbbbbbbbbbbbb3************")

               val topicsSet = topics.toSet
               KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

            } catch {
               case ex: Exception =>

                  throw ex

            }

         case Some(storedOffsets) =>
            try {
               logger.error("***********aaaaaaaaaaaaaaa3************")

               val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
               KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, storedOffsets, messageHandler)

            } catch {
               case ex: Exception =>
                  throw ex

            }

      }
      messages
   }

   def readOffsets(topics: Seq[String], groupId: String, zkUtils: ZkUtils): Option[Map[TopicAndPartition, Long]] = {

      val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicAndPartition, Long]

      val partitionMap = zkUtils.getPartitionsForTopics(topics)

      partitionMap.foreach(topicPartitions => {
         if ((!topicPartitions._1.isEmpty) && (topicPartitions._1 != "") && (!topicPartitions._2.isEmpty) && (topicPartitions._2.toString != "") && (!topicPartitions._2.toString.isEmpty)) {
            val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1) //group,topic
            topicPartitions._2.foreach(partition => {
               val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition

               try {
                  val offsetStatTuple = zkUtils.readData(offsetPath)
                  if (offsetStatTuple != null) {
                           logger.error("***********aaaaaaaaaaaa1 ************")
                     topicPartOffsetMap.put(TopicAndPartition(topicPartitions._1, Integer.valueOf(partition)), offsetStatTuple._1.toLong)
                  }

               } catch {
                  case e: Exception =>
                     topicPartOffsetMap.put(TopicAndPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
                     logger.error("***********bbbbbbbbbbbbbbb1************")
               }
            })
         }
      })
      if (!topicPartOffsetMap.isEmpty && !partitionMap.isEmpty) {
         logger.error("***********aaaaaaaaaaaa2 ************")

         Some(topicPartOffsetMap.toMap)
      } else {
         logger.error("***********bbbbbbbbbbbbbbb2************")

         None
      }
   }
   /*def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean): Unit = {

         val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)

         val acls = new ListBuffer[ACL]()
         val acl = new ACL
         acl.setId(new Id("world", "anyone"))
         acl.setPerms(ZooDefs.Perms.ALL)
         acls += acl

         val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition;
         val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
         zk.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"
           + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))
         logger.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)

   }*/
}
