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

package com.taobao.adfs.distributed;

import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Column;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedElement {
  @Column(width = 50)
  protected String operateIdentifier = null;
  protected Object note = null;

  public String setOperateIdentifier() {
    Invocation currentInvocation = DistributedServer.getCurrentInvocation();
    operateIdentifier = currentInvocation == null ? null : currentInvocation.getIdentifier();
    return operateIdentifier;
  }

  public String setOperateIdentifier(String operateIdentifier) {
    return this.operateIdentifier = operateIdentifier;
  }

  public String getOperateIdentifier() {
    return operateIdentifier;
  }

  public Object setNote(Object note) {
    if (note != null && !(note instanceof String) && !Utilities.isNativeOrJavaPrimitiveType(note))
      throw new RuntimeException("only support primitive type or String for note");
    return this.note = note;
  }

  public Object getNote() {
    return note;
  }

  public boolean isOperateIdentifierMatched() {
    Invocation currentInvocation = DistributedServer.getCurrentInvocation();
    String currentOperateIdentifier = currentInvocation == null ? null : currentInvocation.getIdentifier();
    if (operateIdentifier == null) return false;
    else return operateIdentifier.equals(currentOperateIdentifier);
  }
}
