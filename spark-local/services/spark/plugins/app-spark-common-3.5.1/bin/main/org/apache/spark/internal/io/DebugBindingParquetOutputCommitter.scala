package org.apache.spark.internal.io

import java.io.IOException

import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{BindingPathOutputCommitter, PathOutputCommitter}
import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter

import org.apache.spark.internal.Logging

class DebugBindingParquetOutputCommitter(
    path: Path,
    context: TaskAttemptContext)
  extends BindingParquetOutputCommitter(path, context) with Logging {

  logError(s"Instantiated DebugBindingParquetOutputCommitter with path=$path;")      

  override def getWorkPath(): Path = {
    logError("#getWorkPath")
    super.getWorkPath()
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    logError("#setupTask")    
    super.setupTask(taskAttemptContext)
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    logError("#commitTask")        
    super.commitTask(taskAttemptContext)
  }

  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    logError("#abortTask")           
    super.abortTask(taskAttemptContext)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    logError("#setupJob")           
    super.setupJob(jobContext)
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    logError("#needsTaskCommit")           
    super.needsTaskCommit(taskAttemptContext)
  }

  override def cleanupJob(jobContext: JobContext): Unit = {
    logError("#cleanupJob")       
    super.cleanupJob(jobContext)
  }

  override def isCommitJobRepeatable(jobContext: JobContext): Boolean = {
    logError("#isCommitJobRepeatable")           
    super.isCommitJobRepeatable(jobContext)
  }

  override def commitJob(jobContext: JobContext): Unit = {
    logError("#commitJob")           
    super.commitJob(jobContext)
  }

  override def recoverTask(taskAttemptContext: TaskAttemptContext): Unit = {
    logError("#recoverTask")           
    super.recoverTask(taskAttemptContext)
  }

  override def abortJob(jobContext: JobContext, state: JobStatus.State): Unit = {
    logError("#abortJob")           
    super.abortJob(jobContext, state)
  }

  override def isRecoverySupported: Boolean = {
    logError("#isRecoverySupported")           
    super.isRecoverySupported()
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = {
    logError("#isRecoverySupported")           
    super.isRecoverySupported(jobContext)
  }

}