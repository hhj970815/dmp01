package cn.itcast.agg

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 标签聚合
  */
object TagAgg {

  def agg(graphData: RDD[(VertexId, (ArrayBuffer[String], mutable.Map[String, Double]))])={

    //1、聚合
    val grouped: RDD[(VertexId, Iterable[(ArrayBuffer[String], mutable.Map[String, Double])])] = graphData.groupByKey()
    //2、进行标签权重的累加
    grouped.map(element=>{
      val it = element._2
      //[[52:54:00:7e:02:b6, FPUDNLSGQPWWISKIXFDOVJIYDGNNCFJCSMTAFTHS, VYNDSDBDECCUMPSO],[52:54:00:7e:02:b6, FPUDNLSGQPWWISKIXFDOVJIYDGNNCFJCSMTAFTHS, VYNDSDBDECCUMPSO].[52:54:00:7e:02:b6, FPUDNLSGQPWWISKIXFDOVJIYDGNNCFJCSMTAFTHS, VYNDSDBDECCUMPSO]]
      //进行聚合之后的用户的所有标识
      val alluserIds = it.flatMap(_._1).toBuffer.distinct
      //所有标签
      //[[("aa",1)],[("aa",1),("bb",1)]]
      val allTags: Map[String, Iterable[(String, Double)]] = it.flatMap(_._2).groupBy(_._1)
      val tags = allTags.map(x=>(x._1,x._2.map(_._2).sum))
      //3、数据返回
      (alluserIds.head,(alluserIds,tags))
    })

  }

}
