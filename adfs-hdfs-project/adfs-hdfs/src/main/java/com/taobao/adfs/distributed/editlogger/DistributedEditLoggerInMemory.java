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

package com.taobao.adfs.distributed.editlogger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedEditLoggerInMemory extends DistributedEditLogger {
  public DistributedEditLoggerInMemory(Configuration conf, Object data) throws IOException {
    super(conf, data);
  }

  @Override
  public void appendInternal(Invocation invocation) throws IOException {
    if (invocation == null) throw new IOException("invocation is null");
    applyThreadPoolExecutor.execute(new InvocationRunnable(invocation));
  }

  /**
   * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
   */
  class InvocationRunnable implements Runnable {
    Invocation invocation = null;

    InvocationRunnable(Invocation invocation) {
      this.invocation = invocation;
    }

    @Override
    public void run() {
      try {
        apply(invocation);
      } catch (Throwable t) {
        Utilities.logError(logger, t);
      }
    }
  }
}
