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

import com.taobao.adfs.distributed.DistributedData.DistributedWrite;
import com.taobao.adfs.distributed.DistributedLocker.DistributedLock;

public interface DistributedLockerInternalProtocol {
  @DistributedWrite
  DistributedLock lock(String owner, long expireTime, long timeout, Object... objects) throws IOException;

  @DistributedWrite
  DistributedLock tryLock(String owner, long expireTime, Object... objects) throws IOException;

  @DistributedWrite
  DistributedLock unlock(String owner, Object... objects) throws IOException;
}
