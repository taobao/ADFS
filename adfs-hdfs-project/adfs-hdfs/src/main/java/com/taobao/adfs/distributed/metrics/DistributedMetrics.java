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

package com.taobao.adfs.distributed.metrics;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedServer;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedMetrics implements Updater {
  public static final Logger logger = LoggerFactory.getLogger(DistributedMetrics.class);
  MetricsRecord metricsRecord;
  MetricsRegistry registry = new MetricsRegistry();
  public static final String metricsConfKeyPrefix = "distributed.metrics.conf.";
  static Configuration conf = new Configuration(false);

  public Map<String, MetricsBase> metricsMap = new HashMap<String, MetricsBase>();

  public DistributedMetrics(Configuration conf) {
    DistributedMetrics.conf = (conf == null) ? new Configuration(false) : conf;
  }

  public void open(String recordName) {
    String sessionId = conf.get("session.id");

    // Create a record for distributed metrics
    MetricsContext metricsContext = MetricsUtil.getContext("distributed");
    metricsRecord = MetricsUtil.createRecord(metricsContext, recordName);
    metricsRecord.setTag("sessionId", sessionId);
    metricsContext.registerUpdater(this);
  }

  private MetricsBase getMetrics(Class<? extends MetricsBase> metricClass, String name, String description) {
    try {
      MetricsBase metrics = metricsMap.get(name);
      if (metrics != null) return metrics;
      synchronized (this) {
        metrics = metricsMap.get(name);
        if (metrics != null) return metrics;
        Constructor<? extends MetricsBase> metricsConstructor;
        metricsConstructor = metricClass.getConstructor(String.class, MetricsRegistry.class, String.class);
        if (description == null) description = name;
        metrics = metricsConstructor.newInstance(name, registry, description);
        metricsMap.put(name, metrics);
        return metrics;
      }
    } catch (Throwable t) {
      Utilities.logWarn(logger, "fail to add metrics for ", name, " ", t);
      return null;
    }
  }

  public MetricsTimeVaryingRate getMetricsTimeVaryingRate(String name, String description) {
    return (MetricsTimeVaryingRate) getMetrics(MetricsTimeVaryingRate.class, name, description);
  }

  public MetricsTimeVaryingRate getMetricsTimeVaryingRate(String name) {
    return getMetricsTimeVaryingRate(name, null);
  }

  public MetricsLongValue getMetricsLongValue(String name, String description) {
    return (MetricsLongValue) getMetrics(MetricsLongValue.class, name, description);
  }

  public MetricsLongValue getMetricsLongValue(String name) {
    return getMetricsLongValue(name, null);
  }

  static public void longValueaSet(String name, long newValue) {
    if (DistributedServer.distributedMetrics == null) return;
    DistributedServer.distributedMetrics.getMetricsLongValue(name).set(newValue);
  }

  public MetricsIntValue getMetricsIntValue(String name, String description) {
    return (MetricsIntValue) getMetrics(MetricsIntValue.class, name, description);
  }

  public MetricsIntValue getMetricsIntValue(String name) {
    return getMetricsIntValue(name, null);
  }

  static public void intValueaSet(String name, int newValue) {
    if (DistributedServer.distributedMetrics == null) return;
    DistributedServer.distributedMetrics.getMetricsIntValue(name).set(newValue);
  }

  static public void timeVaryingRateInc(String name, long elapsedTime) {
    if (DistributedServer.distributedMetrics == null) return;
    DistributedServer.distributedMetrics.getMetricsTimeVaryingRate(name).inc(elapsedTime);
  }

  static public void timeVaryingRateIncWithStartTime(String name, long startTime) {
    if (DistributedServer.distributedMetrics == null) return;
    DistributedServer.distributedMetrics.getMetricsTimeVaryingRate(name).inc(System.currentTimeMillis() - startTime);
  }

  public void shutdown() {
  }

  /**
   * Since this object is a registered updater, this method will be called periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public class DistributedActivtyMBean extends MetricsDynamicMBeanBase {
    final private ObjectName mbeanName;

    protected DistributedActivtyMBean(final MetricsRegistry mr) {
      super(mr, "Activity statistics at the distributed");
      mbeanName = MBeanUtil.registerMBean("distributed", "distributedActivity", this);
    }

    public void shutdown() {
      if (mbeanName != null) MBeanUtil.unregisterMBean(mbeanName);
    }
  }
}
