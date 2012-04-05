/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.hadoop.hdfs.metrics;

public interface ZKClientMBean {
  
  /**
   * The ZKClient's up time since it is created
   * @return the number of seconds for up time
   */
  long getUpTime();
  
  /**
   * The total read operations counts since it is created
   * @return the total read counts
   */
  long getTotalReadCounts();
  
  /**
   * The total write operations counts since it is created
   * @return the total write counts
   */
  long getTotalWriteCounts();
  
  /**
   * The total operations counts since it is created
   * @return the total operation counts including write and read
   */
  long getTotalOpsCounts();
  
  /**
   * The average read counts per sec since it is created
   * @return read ops per sec
   */
  long getAverReadCounts();
  
  /**
   * The average write counts per sec since it is created
   * @return write ops per sec
   */
  long getAverWriteCounts();
  
  /**
   * The average counts including read and write 
   * per sec since it is created
   * @return ops per sec
   */
  long getAverOpsCounts();
  
  /**
   * the ratio of R/(R+W) * 100, arranging from 0 to 100
   * @return ratio
   */
  int getRatioRW();
  
}
