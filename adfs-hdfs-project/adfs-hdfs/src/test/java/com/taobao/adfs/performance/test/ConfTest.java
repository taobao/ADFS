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

package com.taobao.adfs.performance.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.taobao.adfs.util.Utilities;

public class ConfTest {
  public static void main(String[] args) throws InterruptedException, IOException {
    Utilities.parseVmArgs(args, null);
    Configuration conf = Utilities.loadConfiguration("distributed-server");
    int repeat = 10000000;
    if (args.length > 0 && args[0] != null) repeat = Integer.valueOf(args[0]);
    conf.set("com.taobao.adfs.performance.test.ConfTest", "true");
    String confTest = null;
    long startTime = System.nanoTime();
    for (int i = 0; i < repeat; ++i) {
      confTest = conf.get("com.taobao.adfs.performance.test.ConfTest");
    }
    long endTime = System.nanoTime();
    double ops = 1000000000.0 * repeat / (endTime - startTime);
    double avg = 1 / ops * 1000;
    System.out.println("ops=" + ops + ", avg=" + avg + "ms, confTest=" + confTest);
  }
}
