package com.zstu.libdata.StreamSplit

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.CommonTools._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
 * Created by SuHanmo on 2017/7/1.
 * AppName: mainAll
 * Function:
 * Input Table:
 * Output Table:
 */
object mainAll {

  val logger = new PrintWriter("./all.txt")
  var day = 0
  var finishedCNKI = false
  var finishedVIP = false
  var finishedWF = false


  def main(args: Array[String]) {
    val hiveContext = initSpark("mainALL")
//    while (true) {
//      val hourNow = refreshDate
//      hourNow match {
//        case 13 => if (!finishedCNKI) run("CNKI",hiveContext)
//        case 15=> if(!finishedVIP) run("VIP", hiveContext)
//        case 14=> if(!finishedWF) run("WF",hiveContext)
//        case _ => Thread.sleep(1000*60*5)
//      }
//    }


    run("VIP", hiveContext)
    run("CNKI", hiveContext)
    run("WF", hiveContext)


  }






  def refreshDate: Int ={
    val today = DateTime.now().dayOfWeek().get()
    if (today != day) {
      finishedCNKI = false
      finishedVIP = false
      finishedWF = false
      day = today
    }
    DateTime.now().hourOfDay().get()
  }


  def run(source: String,hiveContext:HiveContext) = {
    logger.println("its time to run "+source+"!!!" + DateTime.now + "\r\n")
    logger.flush()
    try {
      source match {
        case "VIP" =>
          mainVIP.main(hiveContext:HiveContext)
          finishedVIP = true
        case "WF" =>
          mainWF.main(hiveContext:HiveContext)
          finishedWF = true
        case "CNKI" =>
          mainCNKI.main(hiveContext:HiveContext)
          finishedCNKI = true
        case _ =>
          logger.println("error:uncorrect source name!" + DateTime.now + "\r\n")
          logger.flush()
      }

    } catch {
      case ex: Exception =>
        logger.println("exception" + ex.getMessage + "\ntime:" + DateTime.now + "\r\n")
        logger.flush()
    }
  }

}
