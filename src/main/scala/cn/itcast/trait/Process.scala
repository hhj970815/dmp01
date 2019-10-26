package cn.itcast.`trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

trait Process {
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    * @param spark
    * @param kuduContext
    */
  def process(spark:SparkSession,kuduContext: KuduContext)
}
