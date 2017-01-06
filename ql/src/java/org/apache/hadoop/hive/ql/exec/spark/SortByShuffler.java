/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Iterator;
import java.util.NoSuchElementException;


public class SortByShuffler implements SparkShuffler {
  private final boolean totalOrder;
  private final SparkPlan sparkPlan;
  private final long maxBufferSize;

  /**
   * @param totalOrder whether this shuffler provides total order shuffle.
   */
  public SortByShuffler(boolean totalOrder, SparkPlan sparkPlan, long maxBufferSize) {
    this.totalOrder = totalOrder;
    this.sparkPlan = sparkPlan;
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public JavaPairRDD<HiveKey, Iterable<BytesWritable>> shuffle(
      JavaPairRDD<HiveKey, BytesWritable> input, int numPartitions) {
    JavaPairRDD<HiveKey, BytesWritable> rdd;
    if (totalOrder) {
      if (numPartitions > 0) {
        if (numPartitions > 1 && input.getStorageLevel() == StorageLevel.NONE()) {
          input.persist(StorageLevel.DISK_ONLY());
          sparkPlan.addCachedRDDId(input.id());
        }
        rdd = input.sortByKey(true, numPartitions);
      } else {
        rdd = input.sortByKey(true);
      }
    } else {
      Partitioner partitioner = new HashPartitioner(numPartitions);
      rdd = input.repartitionAndSortWithinPartitions(partitioner);
    }
    return rdd.mapPartitionsToPair(new ShuffleFunction(maxBufferSize));
  }

  @Override
  public String getName() {
    return "SortBy";
  }

  private static class ShuffleFunction implements
      PairFlatMapFunction<Iterator<Tuple2<HiveKey, BytesWritable>>,
          HiveKey, Iterable<BytesWritable>> {
    // make eclipse happy
    private static final long serialVersionUID = 1L;
    private final long maxBufferSize;

    ShuffleFunction(long maxBufferSize) {
      this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<Tuple2<HiveKey, Iterable<BytesWritable>>> call(
      final Iterator<Tuple2<HiveKey, BytesWritable>> it) throws Exception {

      return new Iterator<Tuple2<HiveKey, Iterable<BytesWritable>>>() {
        HiveKey curKey = null;
        HiveBytesWritableCache curValues = new HiveBytesWritableCache(maxBufferSize);

        @Override
        public boolean hasNext() {
          return it.hasNext() || curKey != null;
        }

        @Override
        public Tuple2<HiveKey, Iterable<BytesWritable>> next() {
          if (!hasNext()) {
            throw new NoSuchElementException("No more next");
          }
          while (it.hasNext()) {
            Tuple2<HiveKey, BytesWritable> pair = it.next();
            if (curKey != null && !curKey.equals(pair._1())) {
              HiveKey key = curKey;
              HiveBytesWritableCache values = curValues;
              values.startRead();
              curKey = pair._1();
              curValues = new HiveBytesWritableCache(maxBufferSize);
              curValues.add(pair._2());
              return new Tuple2<HiveKey, Iterable<BytesWritable>>(key, values);
            }
            curKey = pair._1();
            curValues.add(pair._2());
          }
          if (curKey == null) {
            throw new NoSuchElementException();
          }
          // if we get here, this should be the last element we have
          HiveKey key = curKey;
          curKey = null;
          curValues.startRead();
          return new Tuple2<HiveKey, Iterable<BytesWritable>>(key, curValues);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }

      };
    }
  }

}
