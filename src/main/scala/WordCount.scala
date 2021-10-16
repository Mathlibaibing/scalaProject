/**
 * @author libaibing l00423184
 * @date 2021/9/25
 * @description
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
  private val conf = new SparkConf()
  conf.setMaster("local").setAppName("wc")
  private val sparkContext = new SparkContext(conf)
  private val file: String = WordCount.getClass.getClassLoader.getResource("").getFile

  //RDD 是spark的核心 hello nihao ,RDD核心   数据操作
  private val line: RDD[String] = sparkContext.textFile(file)

  //数据转换
  private val word: RDD[String] = line.flatMap(x => x.split(" ") )

  private val wordAnd1 = word.map(x => (x, 1))

  private val result = wordAnd1.reduceByKey((x,y) => {x + y})

  line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).foreach(println)

  result.foreach(println)
}
