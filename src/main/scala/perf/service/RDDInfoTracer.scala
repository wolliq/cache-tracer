package org.apache.spark.rdd

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class RDDInfoTracer[T](spark: SparkSession, logger: Logger, rdd: RDD[T]){

  def explainRDD = {
    logger.info(s"Parent of RDD${rdd.id}:\n" +
      s"parentID: ${rdd.parent(0).id}\n" +
      s"parentDependencies: ${
        if(!rdd.parent(0).getNarrowAncestors.isEmpty) rdd.parent(0).getNarrowAncestors.mkString(" > ")
        else("no narrow ancestors for parent!")
      }\n" +
      s"parentStorageLevel: ${rdd.parent(0).getStorageLevel}\n" +
      s"parentNumPartitions: ${rdd.parent(0).getNumPartitions}\n" +
      s"rddIsCached: ${spark.sparkContext.getPersistentRDDs.seq.contains(rdd.id)}\n" +
      s"parentIsCached: ${spark.sparkContext.getPersistentRDDs.seq.contains(rdd.parent(0).id)}\n" +
      s"RDD_DBG_STRING:\n" + rdd.toDebugString + "\n" +
      s"RDD_PARENT_DBG_STRING:\n ${rdd.parent(0).toDebugString}")
  }
}