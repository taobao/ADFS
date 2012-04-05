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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class ZKMetrics implements Updater {
  private static Log LOG = LogFactory.getLog(ZKMetrics.class);
  private final MetricsRecord metricsRecord;
  public MetricsRegistry registry = new MetricsRegistry();
  private ZKActivtyMBean zkActivityMBean;

  public MetricsTimeVaryingRate ReadRate = new MetricsTimeVaryingRate(
      "ZKReadRate", registry);
  public MetricsTimeVaryingRate SyncWriteRate = new MetricsTimeVaryingRate(
      "ZKSyncWriteRate", registry);
  public MetricsTimeVaryingRate AsycnWriteRate = new MetricsTimeVaryingRate(
      "ZKAsyncWriteRate", registry);
  public MetricsTimeVaryingInt numZKCreateOps = 
      new MetricsTimeVaryingInt("ZKCreateOps", registry);
  public MetricsTimeVaryingInt numZKDeleteOps = 
      new MetricsTimeVaryingInt("ZKDeleteOps", registry);
  public MetricsTimeVaryingInt numZKExistOps = 
      new MetricsTimeVaryingInt("ZKExistOps", registry);
  public MetricsTimeVaryingInt numZKGetChildrenOps = 
      new MetricsTimeVaryingInt("ZKGetChildrenOps", registry);
  public MetricsTimeVaryingInt numZKGetDataOps = 
      new MetricsTimeVaryingInt("ZKGetDataOps", registry);
  public MetricsTimeVaryingInt numZKSetDataOps = 
      new MetricsTimeVaryingInt("ZKSetDataOps", registry);
  public MetricsTimeVaryingInt numZKSimpleLockOps = 
      new MetricsTimeVaryingInt("ZKSimpleLockOps", registry);
  public MetricsTimeVaryingInt numZKSimpleUnlockOps = 
      new MetricsTimeVaryingInt("ZKSimpleUnlockOps", registry);
  
  public ZKMetrics(Configuration conf) {
    // Create a record for ZKCLient metrics
    String sessionId = conf.get("session.id");
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "ZKClient");
    metricsRecord.setTag("sessionId", sessionId);
    metricsContext.registerUpdater(this);
    LOG.info("Initializing ZKMetrics using context object:"
        + metricsContext.getClass().getName());

    // Now the Mbean for the ZooKeeper - this alos registers the MBean
    zkActivityMBean = new ZKActivtyMBean(registry);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (zkActivityMBean != null)
      zkActivityMBean.shutdown();
  }

}
