package com.surfilter.Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.slf4j.LoggerFactory

/**
  * Created by zhouning on 2017/6/12.
  */

private[surfilter] class DateTimeUtils {


    private val logger = LoggerFactory.getLogger("DateTimeProcessTool")


    def datetimeDuration(t1: String, t2: String): Long = {
        val gap = t2.toLong - t1.toLong
        gap
    }

    def stringTodatetimeV1(time: String): Long = {
        try {
            val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")

            val string2long = time.toLong
            val dateTime = new Date(string2long * 1000)
            simpleDateFormat.format(dateTime).toLong


        }
        catch {

            case ex: Exception =>
                logger.warn("********** stringTodatetimeV1 function date conversion exists exception ！**********")
                throw ex
        }

    }

    def stringTodatetimeV2(time: String): String = {
        try {

            val simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
            val string2long = time.toLong
            val dateTime = new Date(string2long * 1000)
            simpleDateFormat.format(dateTime)

        }
        catch {

            case ex: Exception =>
                logger.warn("********** stringTodatetimeV2 function date conversion exists exception ！**********")
                throw ex
        }

    }

    def datetimeminus(date: Date, day: Int): Date = {

        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_MONTH, day)

        calendar.getTime

    }

    def datetime2string(time: Date): String = {
        try {
            val simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
            simpleDateFormat.format(time)

        } catch {
            case ex: Exception =>

                logger.warn("********** datetime2string function date conversion exists exception ！**********")
                throw ex
        }
    }
    def getTimeOfDayV1(startTime: String,endTime:String): String = {
        try {
            val dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
            val start = dateFormat.parse(startTime)
            val end = dateFormat.parse(endTime)
            val calendar1 = Calendar.getInstance()
            val calendar2 = Calendar.getInstance()
            calendar1.setTime(start)
            calendar2.setTime(end)
            val interval =  (calendar1.get(Calendar.HOUR_OF_DAY)+calendar1.get(Calendar.HOUR_OF_DAY))/2
            val result = interval  match  {
                case hour if (0 <= hour && hour < 6) => "darn"
                case hour if (6 <= hour && hour < 12) => "morning"
                case hour if (12 <= hour && hour < 18) => "afternoon"
                case hour if (18 <= hour && hour <= 23) => "night"
            }
            result

        } catch {
                 case ex: Exception =>

                logger.warn("********** get datetime fail ！**********")
                throw ex
        }
    }

    def getTimeOfDayV2(startTime: String): String = {
        try {
            val dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
            val start = dateFormat.parse(startTime)
            val calendar = Calendar.getInstance()
            calendar.setTime(start)

            val interval =  calendar.get(Calendar.HOUR_OF_DAY)
            val result = interval  match  {
                case hour if (0 <= hour && hour < 6) => "darn"
                case hour if (6 <= hour && hour < 12) => "morning"
                case hour if (12 <= hour && hour < 18) => "afternoon"
                case hour if (18 <= hour && hour <= 23) => "night"
            }
            result

        } catch {
            case ex: Exception =>

                logger.warn("********** get datetime fail ！**********")
                throw ex
        }
    }

    def getDayOfWeek(startTime: String): String = {
        try {
            val dateFormat = new SimpleDateFormat("yyyyyMMdd HH:mm:ss")
            val start = dateFormat.parse(startTime)

            val calendar = Calendar.getInstance()
            calendar.setTime(start)

            val result = calendar.get(Calendar.DAY_OF_WEEK)  match  {

                case hour if (hour == 2) => "monday"
                case hour if (hour == 3) => "tuesday"
                case hour if (hour == 4) => "wednesday"
                case hour if (hour == 5) => "thursday"
                case hour if (hour == 6) => "friday"
                case hour if (hour == 1||hour==7) => "weekend"
            }
            result

        } catch {
            case ex: Exception =>

                logger.warn("********** get datetime fail ！**********")
                throw ex
        }
    }
}
object DateTimeUtils{
    def apply(): DateTimeUtils = new DateTimeUtils()
}
