package cn.itcast.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 通过图计算进行用户统一识别
  */
object UserGraph {

  def graph(userData:RDD[(String, (ArrayBuffer[String], mutable.Map[String, Double]))])={
    //1、构建点
    val verties: RDD[(Long, (ArrayBuffer[String], mutable.Map[String, Double]))] = userData.map(element => {
      element match {
          //用户的唯一标识作为每一个点
        case (userid, (allUserIds, tags)) =>
          (userid.hashCode.toLong, (allUserIds, tags))
      }
    })
    //2、构建边
    val edge = userData.flatMap(element=>{
      val list = new ArrayBuffer[Edge[Int]]()
      //遍历用户所有标识，将每一个标识与用户唯一标识都可以组成一个边
      element match {
        case (userid,(allUserIds,tags))=>
          allUserIds.foreach(id=>{

            list.append(Edge(userid.hashCode.toLong,id.hashCode.toLong,0))
          })
      }
      list
    })
    //3、创建graph对象
    val graph = Graph(verties,edge)
    //4、构建连通图
    val components = graph.connectedComponents()
    //5、获取连通图中点集合
    //(userid,aggid)
    val connVerties = components.vertices
    //6、通过连通图点的集合 join 第一步构建的点 补充用户信息
    //(userid,aggid)  join (userid,(alluserids,tags)) => (userid,(aggid,(alluserids,tags)))
    val joinData: RDD[(VertexId, (VertexId, (ArrayBuffer[String], mutable.Map[String, Double])))] = connVerties.join(verties)
    //7、数据返回
    joinData.map(element=>{
      element match {
        case (userid,(aggid,(alluserIds,tags)))=>
          (aggid,(alluserIds,tags))
      }
    })
  }
}
