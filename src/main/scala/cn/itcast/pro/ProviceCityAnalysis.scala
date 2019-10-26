package cn.itcast.pro

import cn.itcast.`trait`.Process
import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * 统计省市地域的分布情况
  */
object ProviceCityAnalysis extends Process{
  //指明数据读取表
  val source_table = s"ODS_${DateUtils.getNow()}"
  //指明数据存入表
  val SINK_TABLE = s"provice_city_analysis_${DateUtils.getNow()}"
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    *
    * @param spark
    * @param kuduContext
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    import org.apache.kudu.spark.kudu._
    //1、读取ODS数据
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",source_table)
      .kudu
      .createOrReplaceTempView("ods")
    //2、统计
     val result = spark.sql(
       """
         |select provincename,cityname,count(1) as number
         |  from ods
         |  group by provincename,cityname
       """.stripMargin)
    //3、将统计结果写入kudu
    //指明表的主键字段
    val keys = Seq[String]("provincename","cityname")
    //指明表的属性 分区策略 分区数据 副本数
    val options = new CreateTableOptions
    import scala.collection.JavaConverters._
    options.addHashPartitions(keys.asJava,3)
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,SINK_TABLE,result,result.schema,keys,options)
  }
}
