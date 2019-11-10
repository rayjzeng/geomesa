/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStore, Query, Transaction}
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreFactory}
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import com.datastax.driver.core.Row
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.utils.ByteBufferUtil


class CassandraSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    CassandraDataStoreFactory.canProcess(params)
  }

  def rdd(
      conf: Configuration,
      sc: SparkContext,
      dsParams: Map[String, String],
      origQuery: Query): SpatialRDD = {

    val ds = DataStoreConnector[CassandraDataStore](dsParams)
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qps = {
      CassandraJobUtils.getMultiStatementPlans(ds, origQuery)
    }

    lazy val _ = qps.map { qp =>
      val tablename = qp.tables.head  // each query plan has a single table

    }

    // Base implementation from GeoToolsSpatialRDDProvider
    WithStore[DataStore](dsParams) { ds =>
      val (sft, features) = WithClose(ds.getFeatureReader(origQuery, Transaction.AUTO_COMMIT)) { reader =>
        (reader.getFeatureType, CloseableIterator(reader).toList)
      }
      SpatialRDD(sc.parallelize(features), sft)
    }
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd rdd
    * @param writeDataStoreParams params
    * @param writeTypeName type name
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {

    // Base implementation from GeoToolsSpatialRDDProvider
    val params = writeDataStoreParams
    val typeName = writeTypeName
    WithStore[DataStore](params) { ds =>
      require(ds != null, "Could not load data store with the provided parameters")
      require(ds.getSchema(typeName) != null, "Schema must exist before calling save - use `DataStore.createSchema`")
    }

    rdd.foreachPartition { iter =>
      WithStore[DataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
