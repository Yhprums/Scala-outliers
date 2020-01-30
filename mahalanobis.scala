package org.apache.spark.mllib.linalg

import breeze.linalg.inv
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{Matrix => BM, Vector => BV}


object mahalanobis {
  def main(args: Array[String]) {

    val master = "local"
    val appName = "mahalanobis"
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = ss.sparkContext

    var dataframe = ss.read.format("csv").load("src/main/resources/data.csv")
    var correctDataframe = dataframe.na.drop() //deletes rows with null elements

    //convert dataframe to rdd and make every number into double
    val rddd = correctDataframe.rdd.map(row => { row.toString() })
    var todouble = rddd.map(m => makeDouble(m))
    val data = todouble.map(a => Array(a(0),a(1)))

    //find mahalanobis distance of every point and print the distinct rdd sorted in descending mode
    val res: RDD[(Vector, Double)] = maha(sc, data.map(Vectors.dense))
        res.distinct.sortBy(-_._2).take(20).foreach(println)

//takes only the distances and finds their mean, min,max,deviations etc
//    val mahaStats = new RowMatrix(res.map(_._2).map(Vectors.dense(_)))
//      .computeColumnSummaryStatistics()
//
//    val mahaMean = mahaStats.mean.toArray.head
//    val mahaVariance = mahaStats.variance.toArray.head
//    val mahaStandardDeviation = scala.math.sqrt(mahaVariance)
//
//    println(mahaStandardDeviation)
//    res.filter(x => scala.math.abs(x._2 - mahaMean) > mahaStandardDeviation).sortBy(-_._2).foreach(println)
  }

  def maha(sc: SparkContext, data: RDD[Vector]) :RDD[(Vector, Double)] = {
    val rm = new RowMatrix(data)
    val covariance: Matrix = rm.computeCovariance() //finds covariance matrix
    val rddStats = rm.computeColumnSummaryStatistics()//finds min, max, mean, deviation

    val mean = asBreeze(rddStats.mean)

    //convert covariance in something breeze can understand + inverse the matrix
    val sInv = inv(new breeze.linalg.DenseMatrix(covariance.numRows, covariance.numCols, covariance.toArray))

    //finds the mahalanobis distances
    val res = data
      .map(asBreeze)
      .map(v => {
        val diffV = v - mean
        val v2 = sInv * diffV
        val dSquared = diffV.t * v2
        val sqrt = Math.sqrt(dSquared)
        (fromBreeze(v), sqrt)
      })

    (res)
  }

  //converts the type of the input
  def fromBreeze(breezeVector: BV[Double]): Vector = {
    Vectors.fromBreeze( breezeVector )
  }

  //converts the type of the input
  def asBreeze(vector: Vector): BV[Double] = {
    vector.asBreeze
  }

  //converts the type of the input
  def asBreeze(m: Matrix): BM[Double] = {
    m.asBreeze
  }

  // deletes the unwanted characters and splits the string in 2 double numbers
  def makeDouble (s: String): Array[Double] = {
    var str = s.replace("[", "").replace("]", "")
    var str2 = str.split(",")
    return Array( str2(0).toDouble, str2(1).toDouble)
  }

  //finds the type of t
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
}
