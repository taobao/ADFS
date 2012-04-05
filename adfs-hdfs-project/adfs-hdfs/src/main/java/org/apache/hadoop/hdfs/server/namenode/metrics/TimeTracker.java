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

 package org.apache.hadoop.hdfs.server.namenode.metrics;
import java.util.*;

public class TimeTracker implements TimeTrackerAPI {
  
  protected static class AnaTimeUnit {
    private final String subopsname;
    private final long elapsedtime;
    
    AnaTimeUnit(String name, long etime) {
      subopsname = name;
      elapsedtime = etime;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(subopsname);
      sb.append("=");
      sb.append(elapsedtime);
      sb.append(";");
      return sb.toString();
    }
  }
  
  private String opsname;
  private List<AnaTimeUnit> anaTimeList = 
    new ArrayList<AnaTimeUnit>();
  private long startTime, lastTime;
  
  static long now() {
    return System.currentTimeMillis();
  }
  
  public TimeTracker(String name) {
    opsname = name;
    startTime = lastTime = now();
  }
  
  public void set(String name) {
    long now = now();
    set(name, now-lastTime);
    lastTime = now;
  }
  
  public String close() {
    long total = now() - startTime;
    set("TOTAL", total);
    StringBuilder sb = new StringBuilder();
    sb.append(opsname + "=[");
    for(AnaTimeUnit atu : anaTimeList) {
      sb.append(atu);
    }
    sb.append("]");
    return sb.toString();
  }
  
  private void set(String name, long elapsed) {
    AnaTimeUnit atu = new AnaTimeUnit(name, elapsed);
    anaTimeList.add(atu);
  }
}
