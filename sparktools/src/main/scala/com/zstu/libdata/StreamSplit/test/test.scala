package com.zstu.libdata.StreamSplit.test

/**
  * Created by Administrator on 2017/6/14 0014.
  */


object test {
  def cleanWfIssue(issue: String): Array[String] = {
    var array: Array[String] = null
    if (issue == null) return null
    //2016, 26(8)
    val issues = issue.substring(issue.indexOf(" "), issue.indexOf("("))
    println(issues.length)
    println(issues)
    if (issues.length == 1) {
      val regex =
        """([0-9]{4}),.([\S]+).""".r
      val regex(issue_year, first) = issue
      array = Array(issue_year, first)
    }
    if (issues.length == 2) {
      val regex =
        """([0-9]{4}),.([\S]{1}).([\S]+).""".r
      val regex(issue_year, first, second) = issue
      array = Array(issue_year, first, second)
    }
    if (issues.length == 3) {
      val regex =
        """([0-9]{4}),.([\S]{2}).([\S]+).""".r
      val regex(issue_year, first, second) = issue
      array = Array(issue_year, first, second)
    }
    array
  }
  def main(args: Array[String]): Unit = {
    cleanWfIssue("2016, (2)")
  }
}
