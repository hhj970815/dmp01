package cn.itcast.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  /**
    * 获取当天日期的yyyyMMdd格式的字符串
    * @return
    */
  def getNow():String={
    val date = new Date()
    //20191015   201910
    val formater = FastDateFormat.getInstance("yyyyMMdd")

    formater.format(date)
  }

  /**
    * 获取昨天的yyyyMMdd格式字符串
    * @return
    */
  def getYesterDay():String={
    val date = new Date()

    val calendar = Calendar.getInstance()
    calendar.setTime(date)

    calendar.add(Calendar.DAY_OF_YEAR,-1)

    val formater = FastDateFormat.getInstance("yyyyMMdd")

    formater.format(calendar)
  }
}
