package cn.itcast.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

/**
  * http请求帮助类
  */
object HttpUtils {

  /**
    * http get请求
    * @param url
    * @return
    */
  def get(url:String):String={

    //1、创建httpClient
    val client = new HttpClient()
    //2、创建请求方式
    val getMethod = new GetMethod(url)
    //3、执行请求
    val code = client.executeMethod(getMethod)
    //4、判断返回状态是否为200，如果为200表示请求成功，否则请求失败
    if(code==200){
      //5、返回数据
      getMethod.getResponseBodyAsString
    }else{
      ""
    }

  }

}
