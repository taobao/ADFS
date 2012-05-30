/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.database;

import com.taobao.adfs.database.innodb.InnodbJniClient;

public class InnodbJniClientPermanceTest {
  public static void main(String[] args) {
    InnodbJniClient innodbJniClient = new InnodbJniClient();
    innodbJniClient.open();

    int result = -1;
    int repeat = (args != null && args.length > 0) ? Integer.valueOf(args[0]) : 1000000;
    long startTime = System.nanoTime();
    for (int i = 0; i < repeat; ++i) {
      result = innodbJniClient.insert("nn_state", "file", new Object[] { 0 });
    }
    long elapsedTime = System.nanoTime() - startTime;
    innodbJniClient.close();

    System.out.println("result=" + result);
    System.out.println("repeat=" + repeat);
    System.out.println("totalTime=" + elapsedTime / 1000000.0 + "ms");
    System.out.println("totalOPS =" + 1.0 * repeat / elapsedTime * 1000000000);
    System.out.println("totalRT  =" + 1.0 * elapsedTime / repeat / 1000000.0 + "ms");
  }
}
