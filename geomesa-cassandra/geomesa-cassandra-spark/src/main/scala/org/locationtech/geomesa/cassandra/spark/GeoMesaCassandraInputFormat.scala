package org.locationtech.geomesa.cassandra.spark

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.opengis.feature.simple.SimpleFeature
import org.apache.cassandra.hadoop.cql3.CqlInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat, InputSplit, JobContext, RecordReader, TaskAttemptContext}
import com.datastax.driver.core.Row
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
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Row](taskAttemptContext.getConfiguration)
    new CassandraRecordReader(toFeatures, delegate.createRecordReader(inputSplit, taskAttemptContext))
  }


  /**
   * Record reader that delegates to Cassandra record readers and transforms the key/values coming back into
   * simple features.
   *
   * @param toFeatures results to features
   * @param reader delegate reader
   */
  class CassandraRecordReader(toFeatures: ResultsToFeatures[Row], reader: RecordReader[java.lang.Long, Row])
      extends RecordReader[Text, SimpleFeature] {

    private val key = new Text()

    private var currentFeature: SimpleFeature = _

    override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit =
      reader.initialize(inputSplit, taskAttemptContext)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = {
      if (reader.nextKeyValue()) {
        currentFeature = toFeatures.apply(reader.getCurrentValue)
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
