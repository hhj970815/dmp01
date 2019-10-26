package cn.itcast.history

import cn.itcast.agg.TagAgg
import cn.itcast.graph.UserGraph
import cn.itcast.utils.{ConfigUtils, DateUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/***
  * 读取历史数据，并进行标签衰减
  */
object HistoryTags {
  //定义历史表的table
  val SOURCE_TABLE = s"tags_${DateUtils.getYesterDay()}"
  def process(spark:SparkSession,currentData:RDD[(String, (mutable.Buffer[String], Map[String, Double]))])={

    //1 、读取标签历史表的数据
    import org.apache.kudu.spark.kudu._
    val source: DataFrame = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",SOURCE_TABLE)
      .kudu
    //2、标签衰减
    //标签衰减参考的是牛顿冷却定律
    //正常情况下 标签的衰减系数，每个都应该是不同
    //当前标签权重 = 历史标签权重 * 衰减系数【0.9】
    val histtoryRDD = source.rdd.map(row=>{
      val userId = row.getAs[String]("userid")
      //标识,标识,标识
      val allUserIdStr = row.getAs[String]("allUserId")
      //标签名 -> 1 ,标签名 -> 1
      val tagsStr = row.getAs[String]("tags")
      //用户所有标识
      val userids = allUserIdStr.split(",").toBuffer
      //[CT_广西梧州市 -> 1.0,CT_广西梧州市 -> 1.0,CT_广西梧州市 -> 1.0]
      val tags: Map[String, Double] = tagsStr.split(",")
        .map(x => {
          val arr = x.split(" -> ")
          val tagName = arr.head
          //标签衰减
          val tagAttr = arr.last.toDouble * 0.9
          (tagName, tagAttr)
        }).toMap

      (userId,(userids,tags))
    })
    //3、将历史数据与当天数据进行合并
    val allRdd = currentData.union(histtoryRDD)
    //4、统一用户识别
    val rdd: RDD[(String, (ArrayBuffer[String], mutable.Map[String, Double]))] = allRdd.map(element => {
      element match {
        case (userId, (allUserIds, tags)) => {
          val ids = new ArrayBuffer[String]()
          ids.++=(allUserIds)
          val allTags = new mutable.HashMap[String, Double]
          allTags.++=(tags)
          (userId, (ids, toMap(allTags)))
        }
      }
    })
    val graph = UserGraph.graph(rdd)
    //5、标签聚合
    TagAgg.agg(graph)
    //6、数据返回
  }


  def toMap(tags:mutable.HashMap[String,Double]):mutable.Map[String,Double]={
    tags
  }
}
