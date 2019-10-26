package cn.itcast.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


/**
  * jdbc帮助类
  */
object JdbcUtils {
  /**
    * 通过geohashcode从business_area表中查询商圈列表
    */
  def query(geoHashCode:String)={

    var connection:Connection = null
    var statement:PreparedStatement = null
    try{

      //1、加载驱动
      Class.forName("com.cloudera.impala.jdbc41.Driver")
      //2、获取connection
      connection = DriverManager.getConnection("jdbc:impala://node03:21050/default")
      //3、创建statement
      statement = connection.prepareStatement(s"select areas from businessArea_${DateUtils.getNow()} where geoHash=?")
      //4、执行查询
      statement.setString(1,geoHashCode)
      val resultSet: ResultSet = statement.executeQuery()
      //5、返回结果
      var areas = ""
      while (resultSet.next()){
        areas = resultSet.getString("areas")
      }

      areas
    }catch {
      case e:Exception=>""
    }finally {
      if(statement!=null)
        statement.close()
      if(connection!=null)
        connection.close()
    }

  }
}
