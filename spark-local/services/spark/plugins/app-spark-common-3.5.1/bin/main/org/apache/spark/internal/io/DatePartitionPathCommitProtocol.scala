package org.apache.spark.internal.io.cloud

import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.internal.Logging
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter

class DatePartitionPathCommitProtocol(
    jobId: String,
    dest: String,
    dynamicPartitionOverwrite: Boolean)
  extends PathOutputCommitProtocol(
    jobId,
    dest,
    dynamicPartitionOverwrite) with Logging {

  logError(s"Instantiated org.apache.spark.internal.io.cloud.DatePartitionPathCommitProtocol(2) with job ID=$jobId;" +
    s" destination=$dest;" +
    s" dynamicPartitionOverwrite=$dynamicPartitionOverwrite")      

  def this(jobId: String, dest: String) =
    this(jobId, dest, false)

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    logError("org.apache.spark.internal.io.cloud.DatePartitionPathCommitProtocol(2) setupTask")
    super.setupTask(taskContext)
  }

  override protected def setupCommitter(context: TaskAttemptContext): PathOutputCommitter = {
    logError(s"Setting up org.apache.spark.internal.io.cloud.DatePartitionPathCommitProtocol(2) committer")    
    super.setupCommitter(context)
  }        

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {

    logError(s"### newTaskTempFile ###")
    logError(s"### dir = ${dir}")

    super.newTaskTempFile(taskContext, dir, spec)
  }

  override def newTaskTempFileAbsPath(
    taskContext: TaskAttemptContext,
    absoluteDir: String,
    spec: FileNameSpec): String = {

    logError(s"### newTaskTempFileAbsPath ###")
    logError(s"### dir = ${absoluteDir}")

    super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
  }
}