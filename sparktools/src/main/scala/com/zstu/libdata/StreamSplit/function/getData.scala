package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.commonOps._
import com.zstu.libdata.StreamSplit.function.distinctRdd.distinctInputRdd
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.getCLCRdd
import ReadData.{readDataLog, readDataV3}
import com.zstu.libdata.StreamSplit.kafka.{cnkiClean, commonClean}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by Administrator on 2017/6/4 0004.
  */
object getData {

 case class fullData(id:String, title:String, titleAlt:String, creator:String, creatorAlt:String, creatorAll:String, keyWord:String, keyWordAlt:String, institute:String, instituteAll:String, year:String, journal:String, issue:String, url
    :String,abstractcont:String,abstract_alt:String,page:String,classifications:String,doi:String,issn:String)


  def getFullDataCNKIsql(hiveContext: HiveContext)={
    def cleanSplitChar(str: String) = {
      if(str == null) null
      else str.replace("|!",";")
    }
    hiveContext.udf.register("cleanSplitChar", (str: String) =>if(str !="" && str !=null) cleanSplitChar(str) else null)

    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstCreator(str) else null)
    hiveContext.udf.register("cleanAuthor", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanKeyWord(str) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getEnglishAbstract(str) else null)
    hiveContext.sql("select " +
      "GUID as id,year,issue,url,page,subject as classifications,DOI,ISSN," +
      "cleanTitle(title) as title," +
      "getFirstCreator(cleanAuthor(creator)) as creator," +
      "cleanSplitChar(creator_all) as creatorAlt," +
      "cleanAuthor(creator) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cleanInstitute(institute) as instituteAll," +
      "getFirstInstitute(cleanInstitute(institute)) as institute," +
      "cleanJournal(journal) as journal," +
      "getChineseAbstract(abstract) as abstract," +
      "getEnglishAbstract(abstract) as abstractAlt " +
      "from t_orgjournaldataCNKI " +
//      " where status = 0 and year = \'2017\'" +
      "")
  }

  def getFullDataVIPsql(hiveContext: HiveContext)={

    def cleanSplitChar(str: String) = {
      if(str == null) null
      else str.replace("|!",";")
    }

    hiveContext.udf.register("cleanSplitChar", (str: String) =>if(str !="" && str !=null) cleanSplitChar(str) else null)

    def getPage(issue: String) = commonClean.cleanIssue(issue,4)(2)
    def getVolume(issue: String) = commonClean.cleanIssue(issue,4)(1)
    def getIssue(issue: String) = commonClean.cleanIssue(issue,4)(0)
    hiveContext.udf.register("getPage", (str: String) =>if(str !="" && str !=null) getPage(str) else null)
    hiveContext.udf.register("getIssue", (str: String) =>if(str !="" && str !=null) getIssue(str) else null)
    hiveContext.udf.register("getVolume", (str: String) =>if(str !="" && str !=null) getVolume(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstCreator(str) else null)
    hiveContext.udf.register("cleanAuthor", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanKeyWord(str) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getEnglishAbstract(str) else null)
    hiveContext.sql("select " +
      "GUID as id,year,url,subject_2 as classifications," +
      "getPage(issue) as page," +
      "getIssue(issue) as issue," +
      "getVolume(issue) as volume," +
      "cleanTitle(title) as title," +
      "title_alt as titleAlt," +
      "getFirstCreator(cleanAuthor(creator_2)) as creator," +
      "cleanSplitChar(creator_all) as creatorAlt," +
      "cleanAuthor(creator_2) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cleanInstitute(institute_2) as instituteAll," +
      "getFirstInstitute(cleanInstitute(institute_2)) as institute," +
      "cleanJournal(journal_name) as journal," +
      "getChineseAbstract(abstract) as abstract," +
      "getEnglishAbstract(abstract) as abstractAlt " +
      "from t_orgjournaldataVIP " +
//      " where status = 0 and year = \'2017\'" +
      "")
  }




  def getFullDataWFsql(hiveContext: HiveContext)={
//    def getPage(issue: String) = commonClean.cleanIssue(issue,8)(2)
    def getVolume(issue: String) = commonClean.cleanIssue(issue,8)(1)
    def getIssue(issue: String) = commonClean.cleanIssue(issue,8)(2)
    def cleanSplitChar(str: String) = {
      if(str == null) null
      else str.replace("|!",";")
    }

    hiveContext.udf.register("cleanSplitChar", (str: String) =>if(str !="" && str !=null) cleanSplitChar(str) else null)

//    hiveContext.udf.register("getPage", (str: String) =>if(str !="" && str !=null) getPage(str) else null)
    hiveContext.udf.register("getIssue", (str: String) =>if(str !="" && str !=null) getIssue(str) else null)
    hiveContext.udf.register("getVolume", (str: String) =>if(str !="" && str !=null) getVolume(str) else null)

    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstCreator(str) else null)
    hiveContext.udf.register("cleanAuthor", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanKeyWord(str) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) =>if(str !="" && str !=null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) =>if(str !="" && str !=null) cnkiClean.getEnglishAbstract(str) else null)
    hiveContext.sql("select " +
      "GUID as id,year,url,subject as classifications,doi," +
//      "getPage(issue) as page," +
      "getIssue(issue) as issue," +
      "getVolume(issue) as volume," +
      "cleanTitle(title) as title," +
      "title_alt as titleAlt," +
      "getFirstCreator(cleanAuthor(creator)) as creator," +
      "cleanSplitChar(creator_all) as creatorAlt," +
      "cleanAuthor(creator) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cleanInstitute(institute) as instituteAll," +
      "getFirstInstitute(cleanInstitute(institute)) as institute," +
      "cleanJournal(journal_name) as journal," +
      "getChineseAbstract(abstract) as abstract," +
      "getEnglishAbstract(abstract) as abstractAlt " +
      "from t_orgjournaldataWF " +
//      " where status = 0 and year = \'2017\'" +
      "")
  }

















  /**
    * 读取所需数据
    *
    * @param hiveContext hiveContext
    * @return (CLCRdd,authorRdd,simplifiedJournalRdd,sourceCoreRdd,journalMagSourceRdd)
    *
    */
  def readSourceRdd(hiveContext: HiveContext) = {
    val CLCRdd = getCLCRdd(hiveContext)


    val authorRddOld: RDD[((String, String), Any)] = readDataV3("t_Author_withSubjectAndKeyword_kid", hiveContext)
      .map(row =>
        ((row.getString(row.fieldIndex("name"))
          , row.getString(row.fieldIndex("partOrgan"))),
          (row.getString(row.fieldIndex("id")),
            row.getString(row.fieldIndex("subjects")),
              row.getString(row.fieldIndex("keywords")),
            row.getString(row.fieldIndex("keywordAlt"))
          )
          ))


    val authorRdd :RDD[((String, String), Any)] = readDataLog("t_CandidateExpert", hiveContext)
      .map(row =>
        ((row.getString(row.fieldIndex("name"))
          , cutStr(row.getString(row.fieldIndex("organization")),4)),
          (row.getString(row.fieldIndex("kId")),
            row.getString(row.fieldIndex("subject")),
            row.getString(row.fieldIndex("keyword")),
            null)
          )
        )
    val universityData = readDataLog("t_University2015",hiveContext)
    .select("name","province","area","english")
      .withColumnRenamed("english","altOrganization")
      .withColumnRenamed("name","university")
    .withColumnRenamed("area","city")

    val simplifiedJournalRdd = readDataLog("t_UnionResource2017", hiveContext)
        .unionAll(readDataLog("t_UnionResource2016", hiveContext))
      .map(f => transformRdd(f))
    val sourceCoreRdd = readDataLog("t_JournalCore", hiveContext)
      .map(f => (f.getString(f.fieldIndex("name")), "1"))
    val journalMagSourceRdd = readDataLog("t_JournalMag", hiveContext)
      .map(f => (f.getString(f.fieldIndex("MAGTITLE")), f.getString(f.fieldIndex("DATASOURCE"))))
    (CLCRdd, authorRdd, simplifiedJournalRdd, sourceCoreRdd, journalMagSourceRdd,universityData)
  }
  def getCoreData(hiveContext:HiveContext)={
    val sourceCoreData = readDataLog("t_JournalCore", hiveContext)
      sourceCoreData.select("name","isCore").withColumnRenamed("name","journalName")
  }

  def getMagData(hiveContext: HiveContext)={
    val journalMagSourceRdd = readDataLog("t_JournalMag", hiveContext)
      .select("MAGTITLE","DATASOURCE")
    journalMagSourceRdd.withColumnRenamed("MAGTITLE","journalName")
      .withColumnRenamed("DATASOURCE","datasource")
  }
  /**
    *
    * @param data data
    * @return
    */
  def getSimplifiedInputRdd(data: DataFrame): RDD[(String, (String, String, String, String, String, String))] ={
    //    (key, (title, journal, creator, id, institute,year))
    //val key = cutStr(title, 6) + cutStr(journal, 4)
    def getJournal(journal1: String,journal2: String) ={
      if (journal1 == null || journal1 == "" ) {
        journal2
      }else journal1
    }
    distinctInputRdd(
      data.map(r =>
        (
          cutStr(cnkiOps.cleanTitle(getRowString(r,"title")),6) + cutStr(getRowString(r,"journal"),4),
          (cnkiOps.cleanTitle(getRowString(r,"title")),
            cnkiOps.cleanJournal(getRowString(r,"journal")),
            cnkiOps.getFirstCreator(getRowString(r,"creatorAll")),
            getRowString(r,"id"),
            cnkiOps.getFirstInstitute(getRowString(r,"instituteAll")),
            getRowString(r,"year")
          )
        ))
    )
  }

  def getForSplitRdd(data:DataFrame): RDD[(String, ((String, String), (String, String, String, String, String)))] = {
    def cleanKeyword(keyword: String) = {
      if(keyword == null) null
      else keyword.replace("|!",";")
    }
    data
      .map(r =>(
      getRowString(r,"id"),
      ((cleanKeyword(getRowString(r,"creatorAll")), cleanKeyword(getRowString(r,"instituteAll"))),
        (cleanKeyword(getRowString(r,"keyWord")), null,
          getRowString(r,"subject"), getRowString(r,"id"),
          getRowString(r,"journal"))
      )
    ))
  }

  def getFullInputRdd(data:DataFrame): RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String,String))] = {
//    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
//    getRowString(r,"title")
    data.map(r=>
      (
        getRowString(r,"GUID"),(
        getRowString(r,"GUID"),
        getRowString(r,"title"),
        /*getRowString(r,"titleAlt")*/null,
        getRowString(r,"creator"),
       /* getRowString(r,"creatorAlt")*/null,
        getRowString(r,"creator_all"),
        getRowString(r,"keyword"),
        /*getRowString(r,"keyWordAlt")*/null,
        getRowString(r,"institute"),
        /*getRowString(r,"instituteAll")*/null,
        getRowString(r,"year"),
        getRowString(r,"journal"),
        getRowString(r,"issue"),
        getRowString(r,"url"),
       /* getRowString(r,"abstractcont")*/null,
        /*getRowString(r,"abstractcont_alt")*/null
      ,getRowString(r,"page")))
    )
  }





  /**
    * 将错误信息写入t_error,返回正确数据
    *
    * @param simplifiedInputRdd 简化过的输入数据
    * @param hiveContext        hiveContext
    * @return rightRdd
    */
  def getRightRddAndReportError(simplifiedInputRdd: RDD[(String, (String, String, String, String, String, String))], hiveContext: HiveContext)= {
    //errorRDD也是一个集合，存放streamingRDD中的错误数据
    val (errorRDD, rightRdd) = (
      simplifiedInputRdd.filter(f => commonOps.filterErrorRecord(f._2))
      , simplifiedInputRdd.filter(f => !commonOps.filterErrorRecord(f._2)))
    //把errorRDD集合映射成t_error表（sqlserver）中的字段



    (rightRdd,errorRDD.map(value => value._2._4))
  }












  def transformRdd_cnki_source(r:Row) ={

    val id = r.getString(r.fieldIndex("GUID"))
    var title =r.getString(r.fieldIndex("title"))
    val titleAlt = ""
    var creator = r.getString(r.fieldIndex("creator"))
    var creatorAlt = r.getString(r.fieldIndex("creator_all"))
    var creatorAll = r.getString(r.fieldIndex("creator"))
    var keyWord = r.getString(r.fieldIndex("keyword"))
    var keyWordAlt =""
    var instituteAll = r.getString(r.fieldIndex("institute"))
    val year = r.getString(r.fieldIndex("year"))
    var journal =r.getString(r.fieldIndex("journal"))
    var institute =r.getString(r.fieldIndex("institute"))
    val issue = r.getString(r.fieldIndex("issue"))
    val url = r.getString(r.fieldIndex("url"))
    var abstractcont =  r.getString(r.fieldIndex("abstract"))
    var abstractcont_alt = r.getString(r.fieldIndex("abstract"))
    val page = r.getString(r.fieldIndex("page"))
    val subject = r.getString(r.fieldIndex("subject"))

    val doi = r.getString(r.fieldIndex("DOI"))
    val issn = r.getString(r.fieldIndex("ISSN"))

    if( title!="" && title !=null) title = cnkiClean.cleanTitle( title)
    if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
    if(creatorAlt !="" && creatorAlt !=null) creatorAlt = cnkiClean.cleanAuthor(creatorAlt)
    if(creatorAll !="" && creatorAll !=null) creatorAll = cnkiClean.cleanAuthor(creatorAll)
    if(keyWord !="" && keyWord !=null) keyWord = cnkiClean.cleanKeyWord(keyWord)
    if(keyWordAlt !="" && keyWordAlt !=null) keyWordAlt = cnkiClean.cleanKeyWord(keyWordAlt)
    if( instituteAll!="" && instituteAll !=null) instituteAll = cnkiClean.cleanInstitute(instituteAll)
    if( journal!="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if( institute!="" && institute !=null) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    if(abstractcont !="" && abstractcont !=null) abstractcont = cnkiClean.getChineseAbstract(abstractcont)
    if(abstractcont_alt !="" && abstractcont_alt !=null) abstractcont_alt = cnkiClean.getEnglishAbstract(abstractcont_alt)
    //logUtil("---abstractcont-"+abstractcont)
    //logUtil("---abstractcont_alt-"+abstractcont_alt)

    fullData(id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt,page,subject,doi,issn)
  }





}
