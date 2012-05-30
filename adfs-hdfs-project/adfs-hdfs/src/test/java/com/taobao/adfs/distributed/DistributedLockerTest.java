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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.distributed.DistributedDataVersion;
import com.taobao.adfs.distributed.DistributedLocker;
import com.taobao.adfs.distributed.DistributedLocker.DistributedLock;
import com.taobao.adfs.util.DeepArray;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedLockerTest {
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
  }

  @Test
  public void unlockALockWhichUnlockedByOtherThread() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt("distributed.locker.remove.expired.lock.interval", 1);
    ReentrantReadWriteLockExtension getDataLocker = new ReentrantReadWriteLockExtension();

    DistributedLocker locker = new DistributedLocker(conf, new DistributedDataVersion(), getDataLocker);
    assertThat(locker.lock("A", Long.MAX_VALUE, Long.MAX_VALUE, 1) != null, is(true));
    assertThat(locker.unlock("A", 1) != null, is(true));
    assertThat(locker.getLock(1) == null, is(true));
    assertThat(locker.lock("B", Long.MAX_VALUE, Long.MAX_VALUE, 1) != null, is(true));
    assertThat(locker.unlock("B", 1) != null, is(true));
    assertThat(locker.getLock(1) == null, is(true));

    // case: lock->unlock===>unlock-X->lock
    conf.setInt("distributed.locker.remove.expired.lock.interval", 1);
    assertThat(locker.unlockDirectly(new DistributedLock(new DeepArray(2), 0, "C", -locker.version.get() - 1, "C",
        false)) != null, is(true));
    assertThat(locker.getLock(2) != null, is(true));
    assertThat(locker.getLock(2).version == -locker.version.get(), is(true));
    conf.setInt("distributed.locker.remove.expired.lock.delay.time", 0);
    locker.lock("A", Long.MAX_VALUE, Long.MAX_VALUE, 1);
    locker.unlock("A", 1);
    assertThat(locker.getLock(2) == null, is(true));
    conf.setInt("distributed.locker.remove.expired.lock.delay.time", 600000);
  }
}
