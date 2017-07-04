package com.zstu.libdata.StreamSplit.function

/**
 * Created by Administrator on 2016/10/16.
 * 去不可见字符，去各种标点，转换小写
 */
object GetReplacedStr {
  def GetReplacedStr(str : String) : String = {
    val c = Array("\"", "<" , ">" , ",", "。","_",
      "-" , "《" , "》","——", "[", "]",
      "(" , ")" , "!", " "  , "\t", "%",
      "“" , "”" , "‘" , "’", "*", "$",
      "/" , "{" ,  "}", "+", "=", "#",
      "&" , "?" , "？" , "@", "『","』",
      ":" , "：" , "·" , "、", "＇","≠",
      "★" , "℃" , "～" , "—", "．","【",
      "】" , "〈" , "〉" , "^", "☆", "…")
    var str1 = str
    for(s <- str){
      if( s<= 31 || s == 127)
        str1 = str1.replace(s.toString,"")
    }
    c.foreach(x => str1 = str1.replace(x,""))
    str1 = str1.replace("|",";")
    str1 = str1.replace("；",";")
    str1.toLowerCase()
  }
}
