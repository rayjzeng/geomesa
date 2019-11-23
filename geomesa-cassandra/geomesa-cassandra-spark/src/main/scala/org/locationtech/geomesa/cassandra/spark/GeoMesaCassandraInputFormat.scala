package org.locationtech.geomesa.cassandra.spark

import java.util
import java.util.AbstractMap.SimpleImmutableEntry
import java.util.Map.Entry

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
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputFormat, InputSplit, Job, JobContext, Mapper, RecordReader, Reducer, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import com.datastax.driver.core.Row
import org.apache.cassandra.hadoop.ConfigHelper
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures

//
//  TODO:
// - Implement Cassandra adapted RecordReader

class GeoMesaCassandraInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging{
  private val delegate = new CqlInputFormat

  override def getSplits(context: JobContext): util.List[InputSplit] = {
    val splits = delegate.getSplits(context)
    splits
  }

  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Entry[Long, Row]](taskAttemptContext.getConfiguration)
    new CassandraRecordReader(toFeatures, delegate.createRecordReader(inputSplit, taskAttemptContext))
  }


  /**
   * Record reader that delegates to Cassandra record readers and transforms the key/values coming back into
   * simple features.
   *
   * @param toFeatures results to features
   * @param reader delegate reader
   */
  class CassandraRecordReader(toFeatures: ResultsToFeatures[Entry[Long, Row]], reader: RecordReader[java.lang.Long, Row])
      extends RecordReader[Text, SimpleFeature] {

    private val key = new Text()

    private var currentFeature: SimpleFeature = _

    override def initialize(inputSplit: _root_.org.apache.hadoop.mapreduce.InputSplit, taskAttemptContext: _root_.org.apache.hadoop.mapreduce.TaskAttemptContext): Unit =
      reader.initialize(inputSplit, taskAttemptContext)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = {
      if (reader.nextKeyValue()) {
        currentFeature = toFeatures.apply(new SimpleImmutableEntry(reader.getCurrentKey, reader.getCurrentValue))
        key.set(currentFeature.getID)
        true
      } else
        false
    }

    override def getCurrentKey: Text = key

    override def getCurrentValue: SimpleFeature = currentFeature

    override def close(): Unit = reader.close()
  }
}
