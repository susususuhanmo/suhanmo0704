package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.CommonTools.splitStr

/**
  * Created by Administrator on 2017/6/14 0014.
  */


object test {

  def cleanInstitute(institute: String): String ={
    def getStrBefore(str: String):String={
      if(str == null) null
      else {
        val rtn = str.replace("，",",")
        println(rtn)
        if(rtn.indexOf(",") >=0) {
          println(rtn.indexOf(","))
          rtn.substring(0,rtn.indexOf(","))

        }
        else rtn
      }
    }
    if(institute == null) null
    else splitStr(institute).map(getStrBefore(_).trim).reduce(_+";"+_)
  }
  def main(args: Array[String]): Unit = {
    val a = cleanInstitute("敦煌研究院保护研究所，甘肃 敦煌 736200")
    println(a)
    }

}
