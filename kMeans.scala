
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.functions._
import org.apache.spark.ml._

object kMeans {
  def main(args: Array[String]) {

    val master = "local"
    val appName = "kmeans"
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = ss.sparkContext

    var dataframe = ss.read.format("csv").load("src/main/resources/data.csv").toDF("x","y")
    var correctDataframe = dataframe.na.drop() //deletes rows with null elements

    //convert dataframe to double
    val DFtoDouble  = correctDataframe.withColumn("x", correctDataframe("x").cast(DoubleType))
    .withColumn( "y", correctDataframe("y").cast(DoubleType))

    //create a column features in the dataframe that contains both x and y
    val assembler = new VectorAssembler().setInputCols(Array("x", "y" ))
      .setOutputCol("features")
    val connectDF = assembler.transform(DFtoDouble)

    //initialize xtandard scaler
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("Standarized")
      .setWithStd(true)
      .setWithMean(true)

    //create a column Standarized that contains our data after being standarized
    val scalerModel = scaler.fit(connectDF)
    val standarized = scalerModel.transform(connectDF)


//================create csv file with our standarized data================================
//
//    val selectedFeatures2 = standarized.select("Standarized").col("Standarized")
//    val vecToArray2 = udf( (xs: linalg.Vector) => xs.toArray )
//    val dfArr2 = standarized.withColumn("featuresArray" , vecToArray2(selectedFeatures2) )
//    val elements2 = Array("SepaX", "SepaY")
//    val sqlExpr2 = elements2.zipWithIndex.map{ case (alias, idx) => col("featuresArray").getItem(idx).as(alias) }
//
//    val writeFile = dfArr2.select(sqlExpr2 : _*)
//    writeFile.write.format("com.databricks.spark.csv").save("src/main/output/stand.csv")
//
//===============================================================================================

    //initialize a Kmeans model that searchs for 5 centers from the column Standarized
    val kmeans = new KMeans().setK(5).setFeaturesCol("Standarized").setMaxIter(50)
    val kmeansModel = kmeans.fit(standarized)
    val centroids = kmeansModel.clusterCenters
    centroids.foreach(println)
    val predictions = kmeansModel.transform(standarized)

    val standData = predictions.select("Standarized").col("Standarized")
    val centersOfPoints = predictions.select("prediction").col("prediction")

    //find the eucleidian distance every point has from its center and sort it in descending mode
    val distFromCenter = udf((features: Vector, c: Int) => Math.sqrt(Vectors.sqdist(features, kmeansModel.clusterCenters(c))))
    val distancesDF = predictions.withColumn("distanceFromCenter", distFromCenter(standData, centersOfPoints))
    val descDistances = distancesDF.orderBy(desc("distanceFromCenter"))
    descDistances.distinct().show(1000,false)

  }

  //finds the type of t
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

}
