
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.internal.io

import java.io.IOException
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil

import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 *
 * @param jobId the job's or stage's id
 * @param path the job's output path, or null if committer acts as a noop
 * @param dynamicPartitionOverwrite If true, Spark will overwrite partition directories at runtime
 *                                  dynamically. Suppose final path is /path/to/outputPath, output
 *                                  path of [[FileOutputCommitter]] is an intermediate path, e.g.
 *                                  /path/to/outputPath/.spark-staging-{jobId}, which is a staging
 *                                  directory. Task attempts firstly write files under the
 *                                  intermediate path, e.g.
 *                                  /path/to/outputPath/.spark-staging-{jobId}/_temporary/
 *                                  {appAttemptId}/_temporary/{taskAttemptId}/a=1/b=1/xxx.parquet.
 *
 *                                  1. When [[FileOutputCommitter]] algorithm version set to 1,
 *                                  we firstly move task attempt output files to
 *                                  /path/to/outputPath/.spark-staging-{jobId}/_temporary/
 *                                  {appAttemptId}/{taskId}/a=1/b=1,
 *                                  then move them to
 *                                  /path/to/outputPath/.spark-staging-{jobId}/a=1/b=1.
 *                                  2. When [[FileOutputCommitter]] algorithm version set to 2,
 *                                  committing tasks directly move task attempt output files to
 *                                  /path/to/outputPath/.spark-staging-{jobId}/a=1/b=1.
 *
 *                                  At the end of committing job, we move output files from
 *                                  intermediate path to final path, e.g., move files from
 *                                  /path/to/outputPath/.spark-staging-{jobId}/a=1/b=1
 *                                  to /path/to/outputPath/a=1/b=1
 */
class HadoopMapReduceCommitProtocolCustom(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(
    jobId,
    path,
    dynamicPartitionOverwrite) with Logging {    

  logError(s"Instantiated org.apache.spark.internal.io.HadoopMapReduceCommitProtocolCustom(2) with job ID=$jobId;" +
    s" path=$path;" +
    s" dynamicPartitionOverwrite=$dynamicPartitionOverwrite")      


  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    logError("@setupCommitter")
    super.setupCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], spec: FileNameSpec): String = {
    logError("@newTaskTempFile")        
    super.newTaskTempFile(taskContext, dir, spec)
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, spec: FileNameSpec): String = {
    logError("@newTaskTempFileAbsPath")                
    super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
  }

  override protected def getFilename(taskContext: TaskAttemptContext, spec: FileNameSpec): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    logError("@getFilename")      
    super.getFilename(taskContext, spec)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    logError("@setupJob")          
    super.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    logError("@commitJob")             
    super.commitJob(jobContext, taskCommits)
  }

  /**
   * Abort the job; log and ignore any IO exception thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param jobContext job context
   */
  override def abortJob(jobContext: JobContext): Unit = {
    logError("@abortJob")             
    super.abortJob(jobContext)
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    logError("@setupTask")       
    super.setupTask(taskContext)
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    logError("@commitTask")           
    super.commitTask(taskContext)
  }

  /**
   * Abort the task; log and ignore any failure thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param taskContext context
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    logError("@abortTask")      
    super.abortTask(taskContext)
  }
}
