package com.nextmining.course.hadoop.wordcount

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by louie on 2016. 4. 26..
  */
object WordCountSparkJob {

  case class Config(input: String = "",
                    output: String = "")

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("WordCountSparkJob") {
      head("WordCountSparkJob")
      opt[String]('i', "input") required() action((x, c) =>
        c.copy(input = x)) text("The path for job input")
      opt[String]('o', "output") required() action((x, c) =>
        c.copy(output = x)) text("The path for job output")
    }
    val opts = parser.parse(args, new Config()).get

    println("*** input = " + opts.input)
    println("*** output = " + opts.output)

    val sparkConf = new SparkConf().setAppName("WordCountSparkJob")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile(opts.input);
    val words = lines.flatMap(line => line.split(" "))

    //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    val counts = words.map(word => (word, 1)).reduceByKey(_+_)

    counts.saveAsTextFile(opts.output)
  }


}
