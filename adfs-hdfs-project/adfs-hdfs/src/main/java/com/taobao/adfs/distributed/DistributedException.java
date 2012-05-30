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

/**
 * if a DistributedException instance is thrown, it means master should stop service
 * 
 * @author jiwan@taobao.com
 */
public class DistributedException extends IOException {
  private static final long serialVersionUID = 6045354658823621708L;
  protected Object resultForServer = null;
  protected boolean needRestore = false;
  protected boolean refuseCall = false;
  public static final String needRestoreFlag = "DistributedException.needRestoreFlag";
  public static final String refuseCallFlag = "DistributedException.refuseCallFlag";

  public DistributedException(boolean needRestore, String message, Throwable t) {
    super(message + (needRestore ? ":" + needRestoreFlag : ""), t);
    this.needRestore = needRestore;
  }

  public DistributedException(boolean refuseCall, Throwable t) {
    super(refuseCall ? refuseCallFlag : "", t);
    this.refuseCall = refuseCall;
  }

  static public boolean isDistributedException(Throwable t) {
    while (true) {
      if (t instanceof DistributedException) return true;
      if (t.getMessage() != null && t.getMessage().contains(DistributedException.class.getName())) return true;
      t = t.getCause();
      if (t == null) return false;
    }
  }

  static public boolean isNeedRestore(Throwable t) {
    while (true) {
      if (t instanceof DistributedException && ((DistributedException) t).needRestore) return true;
      if (t.getMessage() != null && t.getMessage().contains(needRestoreFlag)) return true;
      t = t.getCause();
      if (t == null) return false;
    }
  }

  static public boolean isRefuseCall(Throwable t) {
    while (true) {
      if (t instanceof DistributedException && ((DistributedException) t).refuseCall) return true;
      if (t.getMessage() != null && t.getMessage().contains(refuseCallFlag)) return true;
      t = t.getCause();
      if (t == null) return false;
    }
  }
}
