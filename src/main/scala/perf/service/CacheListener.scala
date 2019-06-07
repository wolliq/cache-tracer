package perf.service

import org.apache.log4j.Logger
import org.apache.spark.scheduler._

class CacheListener(logger: Logger) extends SparkListener {

  override def onJobStart(jobStart: SparkListenerJobStart){
    logger.info(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit ={
    logger.info(s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit ={
    logger.info(s"${environmentUpdate.environmentDetails}")
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated){
    logger.info(s"onBlockUpdated: ${blockUpdated.blockUpdatedInfo.toString}")
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    logger.info(s"onBlockManagerAdded:\n" +
      s"blockManagerId: ${blockManagerAdded.blockManagerId}" +
      s"maxMem: ${blockManagerAdded.maxMem}" +
      s"maxOffHeapMem: ${blockManagerAdded.maxOffHeapMem}" +
      s"maxOnHeapMem: ${blockManagerAdded.maxOnHeapMem}" +
      s"time: ${blockManagerAdded.time}")
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    logger.info(s"executorAdded:\n"  +
      s"executorId: ${executorAdded.executorId}"  +
      s"executorInfo: ${executorAdded.executorInfo}"  +
      s"time: ${executorAdded.time}")
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    logger.info(s"onExecutorMetricsUpdate:\n"  +
      s"executorId: ${executorMetricsUpdate.accumUpdates}"  +
      s"executorInfo: ${executorMetricsUpdate.execId}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info(s"Job ${jobEnd.jobId} ended with ${jobEnd.jobResult} in ${jobEnd.time}.")
  }
}