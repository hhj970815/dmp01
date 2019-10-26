package cn.itcast.utils

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType

/**
  * kudu写入帮助类
  */
object KuduUtils {

  def write2Kudu(kuduContext:KuduContext,
                 tableName:String,
                 data:DataFrame,
                 schema:StructType,keys:Seq[String],
                 option: CreateTableOptions): Unit ={
    //如果表不存在，则创建表，如果存在，则直接写入数据
    if(!kuduContext.tableExists(tableName)){
      kuduContext.createTable(tableName,schema,keys,option)
    }
    //写入数据
    kuduContext.insertRows(data,tableName)
  }
}
