package com.example.spark.commit

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol

/**
 * A FileCommitProtocol that:
 *   1. Flattens Spark's Hive-style partition directory fragment
 *        "yyyy=2026/MM=07/dd=05"  ->  "2026/07/05"
 *   2. Forces the leaf filename to a fixed "file_part.parquet" instead of
 *        Spark's default "part-00000-<jobId>-c000.snappy.parquet"
 *
 * Everything else (binding to the configured S3A committer -- "directory"
 * staging committer in our setup -- local staging, direct final commit to
 * S3, no temp objects) is inherited unchanged from PathOutputCommitProtocol,
 * which is what actually talks to the S3A committer factory.
 *
 * NOTE: getFilename/newTaskTempFile are part of Spark's *internal*
 * (non-public, no binary-compatibility-guaranteed) API, and their signatures
 * HAVE changed between Spark versions -- this version targets Spark 3.5.1,
 * where both methods take a FileNameSpec (prefix/suffix) instead of a plain
 * ext: String (that's a 3.4+ change; earlier 3.x versions use the ext-based
 * signatures from the previous revision of this file). If you move to a
 * different Spark version, re-check
 * org.apache.spark.internal.io.HadoopMapReduceCommitProtocol's source for
 * that version before assuming this still compiles unchanged.
 *
 * FileNameSpec carries a prefix and suffix (suffix includes the compression
 * codec + ".parquet", e.g. ".snappy.parquet") that Spark computes based on
 * the output format/options -- we preserve spec.suffix so the correct
 * extension/codec still ends up in the filename, and just replace the
 * "part-00000-<jobId>" stem with "file_part".
 *
 * Safety: because the DataFrame is repartitioned by ("yyyy","MM","dd")
 * upstream (hash partitioning), every row for a given date is guaranteed to
 * be handled by a single task -- so exactly one task ever writes into a
 * given yyyy/MM/dd directory, and the fixed filename never collides.
 * If you can't guarantee that upstream, fall back to including
 * taskContext.getTaskAttemptID.getTaskID.getId in the filename.
 */
class FlatKeyStagingCommitProtocol(
    jobId: String,
    dest: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends PathOutputCommitProtocol(jobId, dest, dynamicPartitionOverwrite) {

  private def flattenDir(dir: Option[String]): Option[String] =
    dir.map(_.split("/").map(_.split("=", 2).last).mkString("/"))

  override protected def getFilename(
      taskContext: TaskAttemptContext,
      spec: FileNameSpec): String = {
    s"${spec.prefix}file_part${spec.suffix}"
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {
    super.newTaskTempFile(taskContext, flattenDir(dir), spec)
  }
}