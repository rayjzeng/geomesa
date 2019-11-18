package org.locationtech.geomesa.cassandra.spark

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.geotools.data.{DataStore, Query, Transaction}
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreFactory, CassandraQueryPlan, EmptyPlan}
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils
import org.locationtech.geomesa.index.api.QueryPlan
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputFormat, InputSplit, Job, JobContext, Mapper, RecordReader, Reducer, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import com.datastax.driver.core.Row
import org.apache.cassandra.hadoop.ConfigHelper

//
//  TODO:
// - Implement Cassandra adapted RecordReader

class GeoMesaCassandraInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging{
  private val delegate = new CqlInputFormat

  override def getSplits(context: JobContext): util.List[InputSplit] = {
    val splits = delegate.getSplits(context)
    splits
  }

  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Text, SimpleFeature] = ???
}
