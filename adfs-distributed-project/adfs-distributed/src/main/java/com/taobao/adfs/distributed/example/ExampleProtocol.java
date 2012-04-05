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

package com.taobao.adfs.distributed.example;

import java.io.IOException;

import com.taobao.adfs.distributed.DistributedLockerInternalProtocol;
import com.taobao.adfs.distributed.DistributedLockerProtocol;
import com.taobao.adfs.distributed.DistributedData.DistributedRead;
import com.taobao.adfs.distributed.DistributedData.DistributedWrite;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public interface ExampleProtocol extends DistributedLockerInternalProtocol, DistributedLockerProtocol {
  // for function test

  @DistributedWrite
  public Object[] format() throws IOException;

  @DistributedWrite
  public String write(String content) throws IOException;

  @DistributedRead
  public String read() throws IOException;

  // for performance test

  @DistributedWrite
  public Object writeRequestInMemory(Object object) throws IOException;

  @DistributedWrite
  public Object writeResultInMemory(Object object) throws IOException;

  @DistributedRead
  public Object readInMemory(Object object) throws IOException;
}
