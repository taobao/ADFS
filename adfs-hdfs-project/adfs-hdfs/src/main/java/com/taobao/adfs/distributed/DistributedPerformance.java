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

package com.taobao.adfs.distributed;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;

import com.taobao.adfs.distributed.rpc.ClassCache;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedPerformance {
  Configuration conf = null;

  /**
   * -Ddistributed.performance.test=true -Ddistributed.data.class.name=com.taobao.adfs.distributed.example.ExampleData
   * -Ddistributed.performance.test.invocations=readInMemory(Object=null) -Ddistributed.performance.test.repeat=1
   * -Ddistributed.performance.test.threads=1 -Ddistributed.manager.address=localhost:2181
   */
  public static void main(String[] args) throws IOException {
    new DistributedPerformance().testEntry(args);
  }

  public void testEntry(String[] args) throws IOException {
    Utilities.parseVmArgs(args, null);
    conf = Utilities.loadConfiguration("distributed-server");
    if (!conf.getBoolean("distributed.performance.test", false)) {
      System.out.println("ignore distributed performance test");
      return;
    }
    String[] invocationStrings = conf.get("distributed.performance.test.invocations", ";").split(";");
    for (String invocationString : invocationStrings) {
      testMethod(invocationString);
    }
    Utilities.logInfo(Utilities.logger, "-------------------------------------------------");
    Utilities.logInfo(Utilities.logger, "");
  }

  static class RepeatThreads {
    private static long repeat = 0;
    private static Invocation invocation = null;
    private static AtomicLong count = new AtomicLong(0);
    private List<RepeatThread> repeatThreads = new ArrayList<RepeatThread>();
    long startTime = 0;
    long endTime = 0;

    RepeatThreads(int threads, long repeat, Invocation invocation) {
      RepeatThreads.repeat = repeat;
      RepeatThreads.invocation = invocation;
      for (int i = 0; i < threads; ++i) {
        repeatThreads.add(new RepeatThread());
      }
    }

    void execute() {
      startTime = System.currentTimeMillis();
      for (int i = 0; i < repeatThreads.size(); ++i) {
        repeatThreads.get(i).setName("RepeatThread-" + i);
        Utilities.logInfo(Utilities.logger, repeatThreads.get(i).getName(), " started");
        repeatThreads.get(i).start();
      }

      double lastCheckTime = startTime;
      long lastCheckCount = 0;
      while (!repeatThreads.isEmpty()) {
        for (RepeatThread repeatThread : repeatThreads.toArray(new RepeatThread[0])) {
          if (!repeatThread.isAlive()) {
            Utilities.logInfo(Utilities.logger, repeatThread.getName(), " finished");
            repeatThreads.remove(repeatThread);
          }
        }

        double currentTime = System.currentTimeMillis();
        double deltaTime = (currentTime - lastCheckTime) / 1000.0;
        double totalTime = (currentTime - startTime) / 1000.0;
        lastCheckTime = currentTime;
        long currentCount = count.get();
        long deltaCount = currentCount - lastCheckCount;
        lastCheckCount = currentCount;
        double currentOps = deltaCount / deltaTime;
        double totalOps = currentCount / totalTime;

        Utilities.logInfo(Utilities.logger, "finishedCount=", String.format("%010d", count.get()), ", elapsedTime=",
            String.format("%010.2f", totalTime), "s, currentOps=", String.format("%010.2f", currentOps), ", totalOps=",
            String.format("%010.2f", totalOps));
        Utilities.sleepAndProcessInterruptedException(100, null);
      }
      endTime = System.currentTimeMillis();
    }

    static class RepeatThread extends Thread {
      RepeatThread() {
        setDaemon(true);
      }

      public void run() {
        while (count.getAndIncrement() < repeat) {
          try {
            invocation.invoke();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    };
  }

  void testMethod(String invocationString) throws IOException {
    int threads = conf.getInt("distributed.performance.test.threads", 1);
    long repeat = conf.getLong("distributed.performance.test.repeat", 0L);
    Invocation invocation = getInvocation(invocationString);
    RepeatThreads repeatThreads = new RepeatThreads(threads, repeat, invocation);
    repeatThreads.execute();
    long totalTime = repeatThreads.endTime - repeatThreads.startTime;

    Utilities.logInfo(Utilities.logger, "");
    Utilities.logInfo(Utilities.logger, "-------------------------------------------------");
    Utilities.logInfo(Utilities.logger, "lastInvocation: " + invocation.toString(true));
    Utilities.logInfo(Utilities.logger, "       threads: " + threads);
    Utilities.logInfo(Utilities.logger, "        repeat: " + repeat);
    Utilities.logInfo(Utilities.logger, "          time: " + totalTime + "ms");
    Utilities.logInfo(Utilities.logger, "      totalOps: " + 1000.0 * repeat / totalTime + "/s");
    Utilities.logInfo(Utilities.logger, "    1/totolOps: " + 1.0 * totalTime / repeat + "ms");
  }

  Invocation getInvocation(String invocationString) throws IOException {
    // get data class
    String dataClassName = conf.get("distributed.data.class.name");
    if (dataClassName == null) {
      Utilities.logError(Utilities.logger, "need to specify distributed.data.class.name");
      throw new IOException("need to specify distributed.data.class.name");
    }
    Class<?> dataProtocol = ClassCache.getWithIOException(dataClassName).getInterfaces()[0];

    // get method name and parameter strings
    String name = invocationString.split("\\(")[0];
    String parameterStrings = invocationString.substring(name.length() + 1, invocationString.length() - 1);
    if (!parameterStrings.contains(",")) parameterStrings += ",";
    String[] parameterPairs = parameterStrings.split(",");
    String[] parameterTypeSimpleNames = new String[parameterPairs.length];
    String[] parameterValueStrings = new String[parameterPairs.length];
    DistributedShell.parseParameterTypesAndValues(parameterPairs, parameterTypeSimpleNames, parameterValueStrings);

    // get method
    Method[] methodMatched = DistributedShell.findMethod(dataProtocol.getMethods(), name, parameterTypeSimpleNames);
    if (methodMatched.length == 0) throw new IOException("cannot find method " + name);
    if (methodMatched.length > 1) throw new IOException("two or more methods are matched for " + name);
    Method method = methodMatched[0];

    // get parameter values
    Object[] parameterValues = new Object[method.getParameterTypes().length];
    for (int i = 0; i < parameterValueStrings.length; ++i) {
      parameterValues[i] = DistributedShell.stringToObject(parameterValueStrings[i], method.getParameterTypes()[i]);
    }

    // get invocation
    Invocation invocation = new Invocation(method, parameterValues);
    invocation.setObject(DistributedClient.getClient(conf));
    return invocation;
  }
}
