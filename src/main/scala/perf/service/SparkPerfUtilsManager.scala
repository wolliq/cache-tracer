package perf.service

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SparkPerfUtilsManager(cores: Int = 1, isLocal: Boolean = true, datasetPath: String){

  val spark =
    if(isLocal)
      SparkSession.builder
        .master(s"local[${cores}]")
        .appName("spark-perf-app")
        .getOrCreate()
    else
      SparkSession.builder
        .appName("spark-perf-app")
        .getOrCreate()

  override def toString(): String = s"${this.getClass} => cores: ${cores}\nisLocal: ${isLocal}"
}


object SparkPerfUtilsManager {

  val logger = Logger.getLogger(this.getClass)

  def apply(cores: Int, isLocal: Boolean = true, datasetPath: String): SparkPerfUtilsManager =
    new SparkPerfUtilsManager(cores, isLocal, datasetPath)

  def logRDDInfoMap(rdd: RDD[String], fraction: Double = 0.15){

    val Sep = " *** "

    val sampleRDD = rdd.sample(true, fraction)

    if(!sampleRDD.isEmpty() && sampleRDD.count != 0){
      val sampleRDDsizeInBytes = sampleRDD.map(_.mkString(",").getBytes("UTF-8").length.toLong).reduce(_ + _)
      val sampleRDDsizeInMB: Double = sampleRDDsizeInBytes.toDouble / (1024 * 1024).toDouble
      val sampleAvgRowSizeInByte: Double = sampleRDDsizeInBytes / sampleRDD.count
      val totalRows: Long = rdd.count()
      val estimatedTotalSizeInByte: Double = totalRows.toDouble * sampleAvgRowSizeInByte

      val estimateInMB: Long = (estimatedTotalSizeInByte.toDouble / (1024*1024).toDouble).toLong

      val infoMap = Map(
        "rddID" -> rdd.id,
        "rddDebugString" -> rdd.toDebugString,
        "rddNumPartitions" -> rdd.getNumPartitions,
        "rddStorageLevel" -> rdd.getStorageLevel.toString,
        "rddDeps" -> rdd.dependencies.mkString(","),
        "sampleRDDsizeInMB" -> sampleRDDsizeInMB.toString,
        "sampleAvgRowSizeInByte" -> sampleAvgRowSizeInByte.toString,
        "totalRows" -> totalRows.toString,
        "estimateInMB" -> estimateInMB.toString)

      logger.info(s"Estimated RDD info map:\n${infoMap.toString}")
    }
  }
}
