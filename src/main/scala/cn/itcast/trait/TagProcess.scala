package cn.itcast.`trait`

import scala.collection.mutable

trait TagProcess {
  /**
    * 生成对应的标签，具体逻辑由子类实现
    * @param args
    */
  def makeTag(args:Any*):mutable.HashMap[String,Double]
}
