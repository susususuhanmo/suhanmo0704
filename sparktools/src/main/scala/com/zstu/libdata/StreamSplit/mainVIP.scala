package com.zstu.libdata.StreamSplit

import com.zstu.libdata.StreamSplit.function.getData.{getForSplitRdd, getRightRddAndReportError, readSourceRdd}
import com.zstu.libdata.StreamSplit.function.newDataOps.dealNewData0623
import com.zstu.libdata.StreamSplit.function._
import org.apache.spark.sql.hive.HiveContext

//import com.zstu.libdata.StreamSplit.function.oldDataOps.dealOldData
import com.zstu.libdata.StreamSplit.kafka.commonClean
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.addCLCName
import com.zstu.libdata.StreamSplit.function.printLog.logUtil

/**
  * Created by Administrator on 2017/6/24 0024.
  */
object mainVIP {

  def main(hiveContext:HiveContext): Unit = {

    val types = 4
    var (clcRdd, authorRdd, simplifiedJournalRdd, sourceCoreRdd, journalMagSourceRdd,universityData)
    = readSourceRdd(hiveContext)


    logUtil("数据读取完成")
    logUtil("clc" + clcRdd.count())
    logUtil("author" + authorRdd.count())
    logUtil("simpified" + simplifiedJournalRdd.count())
    logUtil("sourceCore" + sourceCoreRdd.count())
    logUtil("Mag" + journalMagSourceRdd.count())
    //    while(true)
    //      {
    try
    {

      //      val CNKIData = readData165("t_CNKI_UPDATE",hiveContext).limit(3000)
      //    (key, (title, journal, creator, id, institute,year))

      val orgjournaldata = commonClean.readDataOrg("t_VIP_UPDATE", hiveContext)
        .filter("status = 0")
      orgjournaldata.registerTempTable("t_orgjournaldataVIP")



      val logData = hiveContext.sql("select GUID as id,"+types+" as resource from t_orgjournaldataVIP")
      WriteData.writeDataLog("t_Log",logData)

      val fullInputData=  addCLCName(getData.getFullDataVIPsql(hiveContext),clcRdd,hiveContext).cache()



      val simplifiedInputRdd =
        distinctRdd.distinctInputRdd(orgjournaldata.map(f =>commonClean.transformRdd_vip_simplify(f)))






      hiveContext.dropTempTable("t_orgjournaldataVIP")
      // val simplifiedInputRdd =getSimplifiedInputRdd(CNKIData)
      logUtil("简化后的数据" + simplifiedInputRdd.count())
      val forSplitRdd =getForSplitRdd(fullInputData)
      logUtil("待拆分的数据" + forSplitRdd.count())

//      val fullInputRdd  =
//        orgjournaldata.map(f =>commonClean.transformRdd_vip_source(f))


      // TODO: ....................
      //      val fullInputRdd2  =
      //        orgjournaldata.map(f =>getData.transformRdd_cnki_source(f))

      //      val fullInputData = addCLCName(hiveContext.createDataFrame(fullInputRdd2)
      //      .withColumnRenamed("abstractcont","abstract")
      //      .withColumnRenamed("abstract_alt","abstractAlt"),clcRdd,hiveContext)



      // TODO: ....................










      // val fullInputRdd = getFullInputRdd(CNKIData)
//      logUtil("完整字段数据" + fullInputRdd.count())


      //过滤出正常数据并将错误数据反馈
      val (rightInputRdd,errorRdd) = getRightRddAndReportError(simplifiedInputRdd, hiveContext)
      logUtil("正常数据" + rightInputRdd.count())

      WriteData.writeErrorData(errorRdd,types,hiveContext)


      //开始查重 join group
      val inputJoinJournalRdd = rightInputRdd.leftOuterJoin(simplifiedJournalRdd).map(f => (f._2._1._4, f._2))
      logUtil("join成功" + inputJoinJournalRdd.count())
      val joinedGroupedRdd = inputJoinJournalRdd.groupByKey()
      logUtil("group成功" + joinedGroupedRdd.count())


      //处理新数据 得到新的journal大表 和 新作者表
      val newAuthorRdd =
        dealNewData0623(joinedGroupedRdd, fullInputData, sourceCoreRdd
          , journalMagSourceRdd, simplifiedJournalRdd, types
          , authorRdd, clcRdd, hiveContext, forSplitRdd,universityData)
      logUtil("新数据处理成功获得新数据")


      authorRdd = newAuthorRdd
      logUtil("数据更新成功")


      //处理旧数据
//      val num = dealOldData(inputJoinJournalRdd, fullInputRdd, sourceCoreRdd
//        , journalMagSourceRdd, simplifiedJournalRdd, types)
      val num = oldDataOps.dealOldData(fullInputData, types, inputJoinJournalRdd
        , hiveContext)
      logUtil("匹配成功的旧数据处理成功" + num)

    }catch {
      case ex: Exception => logUtil(ex.getMessage)
    }




  }
}
