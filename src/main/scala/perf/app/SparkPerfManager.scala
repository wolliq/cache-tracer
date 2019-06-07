package perf.app

import org.apache.log4j.Logger
import org.apache.spark.rdd.{RDD, RDDInfoTracer}
import org.apache.spark.sql.SparkSession
import perf.service.{CacheListener, SparkPerfUtilsManager}

case class SparkPerfManager(cores: Int = 1, isLocal: Boolean = true)

object SparkPerfManager {

  val logger = Logger.getLogger(this.getClass)

  def createUtilsManager(cores: Int = 1, isLocal: Boolean = true, datasetPath: String) = {
    new SparkPerfUtilsManager(cores, isLocal, datasetPath)
  }

  def main(args: Array[String]): Unit = {

    val (cores: String, isLocal: String, datasetPath: String) = parse(args)

    val utilsManager = createUtilsManager(cores.toInt, isLocal.toBoolean, datasetPath)

    val spark = utilsManager.spark

    val cacheListener = new CacheListener(logger)
    spark.sparkContext.addSparkListener(cacheListener)

    spark.sparkContext.addSparkListener(new CacheListener(logger))
    val data = loadDataset(spark, datasetPath)

    RDDInfoTracer(spark, logger, data).explainRDD

    run(utilsManager, ESTIMATE_RDD_SIZE, data)
  }

  private def parse(args: Array[String]) = {
    val cores = args(0)
    val isLocal = args(1)
    val datasetPath = args(2)
    (cores, isLocal, datasetPath)
  }

  def loadDataset(spark: SparkSession, path: String): RDD[String] = {
    val TestDatasetPath = "./src/test/resources/temp-nets/sx-stackoverflow.txt"
    spark.sparkContext.textFile(TestDatasetPath)
  }

  def run(utilsManager: SparkPerfUtilsManager, action: ProcessAction, data: RDD[String]): Unit = action match {
    case ESTIMATE_RDD_SIZE => SparkPerfUtilsManager.logRDDInfoMap(data)
    case _ => "DEFAULTS"
  }
}
