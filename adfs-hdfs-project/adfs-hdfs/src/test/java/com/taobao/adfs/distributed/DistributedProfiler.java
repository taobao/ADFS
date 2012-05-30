/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 \*   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.distributed;

//import all BTrace annotations
import com.sun.btrace.BTraceUtils;
import com.sun.btrace.aggregation.Aggregation;
import com.sun.btrace.aggregation.AggregationFunction;
import com.sun.btrace.annotations.*;
// import statics from BTraceUtils class
import static com.sun.btrace.BTraceUtils.*;

//@BTrace annotation tells that this is a BTrace program
@BTrace
public class DistributedProfiler {
  private static Aggregation histogram = newAggregation(AggregationFunction.QUANTIZE);

  @OnMethod(clazz = "com.taobao.adfs.distributed.DistributedServer", method = "invoke", location = @Location(Kind.RETURN))
  public static void onInvoke(@Duration long duration) {
    int d = (int) (duration / 1000);
    addToAggregation(histogram, d);
  }

  @OnTimer(1000)
  public static void onTimer() {
    // Top 16 queries only
    BTraceUtils.truncateAggregation(histogram, 16);

    println("---------------------------------------------");
    printAggregation("Histogram", histogram);
    println("---------------------------------------------");
  }
}