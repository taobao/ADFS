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

package com.taobao.adfs.distributed.editlogger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public abstract class DistributedEditLogger {
  public static final Logger logger = LoggerFactory.getLogger(DistributedEditLogger.class);
  Configuration conf = null;
  Object data = null;
  Method invokeMethod = null;
  public AtomicBoolean failToApply = new AtomicBoolean(false);
  ThreadPoolExecutor applyThreadPoolExecutor = null;
  AtomicInteger workSizeInProgressAndInQueue = new AtomicInteger(0);
  AtomicBoolean pauseToApply = new AtomicBoolean(false);
  AtomicBoolean isClosing = new AtomicBoolean(false);

  public static DistributedEditLogger getDistributedEditLogger(Configuration conf, Object data) throws IOException {
    try {
      conf = (conf == null) ? new Configuration(false) : conf;
      Class<?> editLoggerClass = conf.getClass("distributed.edit.log.class.name", DistributedEditLoggerInMemory.class);
      Constructor<?> constructor = editLoggerClass.getConstructor(Configuration.class, Object.class);
      return (DistributedEditLogger) constructor.newInstance(conf, data);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  DistributedEditLogger(Configuration conf, Object data) {
    if (data == null) throw new IllegalArgumentException("data is null");
    this.conf = (conf == null) ? new Configuration(false) : conf;
    this.data = data;
    try {
      invokeMethod = Invocation.getMethod(data.getClass(), "invoke", Invocation.class);
    } catch (Throwable t) {
      Utilities.logInfo(logger, "no invoke method found in ", data.getClass().getName(), ", will call directly");
    }
    open();
  }

  public boolean isEmpty() {
    return getWorkSizeInProgressAndInQueue() == 0;
  }

  public int getWorkSizeInProgressAndInQueue() {
    return workSizeInProgressAndInQueue.get();
  }

  public int append(Invocation invocation) throws IOException {
    appendInternal(invocation);
    return workSizeInProgressAndInQueue.incrementAndGet();
  }

  public void apply(Invocation invocation) {
    try {
      Utilities.waitValue(pauseToApply, false, null, false);
      if (invocation == null) throw new IOException("invocation is null");
      if (invokeMethod == null) invocation.invoke(data);
      else {
        long startTime = System.currentTimeMillis();
        invocation.setResult(invokeMethod.invoke(data, invocation));
        invocation.setElapsedTime(System.currentTimeMillis() - startTime);
      }
      DistributedMetrics.timeVaryingRateInc("dataInvokeByEditLogger." + invocation.getMethodName(), invocation
          .getElapsedTime());
      Utilities
          .logDebug(logger, "succeed in applying 1/", getWorkSizeInProgressAndInQueue(), " edit log: ", invocation);
    } catch (Throwable t) {
      failToApply.set(true);
      Utilities.log(logger, (t instanceof InterruptedException && isClosing.get()) ? Level.DEBUG : Level.ERROR,
          "fail to apply edit log: ", invocation, t);
    } finally {
      workSizeInProgressAndInQueue.getAndDecrement();
      ThreadPoolExecutor oldThreadPoolExecutor = applyThreadPoolExecutor;
      if (applyThreadPoolExecutor != null) {
        DistributedMetrics.intValueaSet("dataInvokeByEditLogger.workSizeInProgress", oldThreadPoolExecutor
            .getActiveCount());
        DistributedMetrics.intValueaSet("dataInvokeByEditLogger.workSizeInQueue", oldThreadPoolExecutor.getQueue()
            .size());
      }
    }
  }

  abstract public void appendInternal(Invocation invocation) throws IOException;

  public void open() {
    close(true);
    workSizeInProgressAndInQueue = new AtomicInteger(0);
    Utilities.logInfo(logger, "opening edit log with ", getWorkSizeInProgressAndInQueue(), " logs");
    failToApply = new AtomicBoolean(false);
    int rpcHandlerNumber = conf.getInt("distributed.server.handler.number", 100);
    applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(rpcHandlerNumber);
    Utilities.logInfo(logger, "opened edit log with ", getWorkSizeInProgressAndInQueue(), " logs");
  }

  public void close(boolean shutdownNow) {
    isClosing.set(true);
    int remainingEditLogNumber = getWorkSizeInProgressAndInQueue();
    Utilities.logInfo(logger, "closing edit log with ", remainingEditLogNumber, " logs");
    if (applyThreadPoolExecutor != null) {
      if (shutdownNow) applyThreadPoolExecutor.shutdownNow();
      else applyThreadPoolExecutor.shutdown();
      while (applyThreadPoolExecutor.getActiveCount() > 0) {
        Utilities.sleepAndProcessInterruptedException(10, logger);
      }
      applyThreadPoolExecutor = null;
    }
    Utilities.logInfo(logger, "closed edit log with ", remainingEditLogNumber, " logs");
    workSizeInProgressAndInQueue.set(0);
    isClosing.set(false);
  }

  public void clear() {
    open();
  }

  public void pauseApply() {
    Utilities.logInfo(logger, "pausing applying with ", getWorkSizeInProgressAndInQueue(), " logs");
    pauseToApply.set(true);
    Utilities.logInfo(logger, "paused applying with ", getWorkSizeInProgressAndInQueue(), " logs");
  }

  public void resumeApply() {
    Utilities.logInfo(logger, "resuming applying with ", getWorkSizeInProgressAndInQueue(), " logs");
    pauseToApply.set(false);
    Utilities.logInfo(logger, "resumed applying with ", getWorkSizeInProgressAndInQueue(), " logs");
  }

  public void waitUntilWorkSizeIsEmpty() {
    int remainingEditLogNumber = getWorkSizeInProgressAndInQueue();
    if (remainingEditLogNumber > 0) Utilities.logInfo(logger, "waiting ", remainingEditLogNumber, " logs");
    while (!isEmpty()) {
      Utilities.sleepAndProcessInterruptedException(10, logger);
    }
    if (remainingEditLogNumber > 0) Utilities.logInfo(logger, "waited ", remainingEditLogNumber, " logs");
  }
}
