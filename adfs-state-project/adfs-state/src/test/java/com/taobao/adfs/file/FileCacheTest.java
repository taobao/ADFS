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

package com.taobao.adfs.file;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.file.File;
import com.taobao.adfs.state.internal.StateManagerInternal;
import com.taobao.adfs.state.internal.StateManagerInternalTest;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class FileCacheTest {
  static StateManagerInternal stateManagerInternal = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
  }

  StateManagerInternal createStateManagerInternal(int cacheCapacity, int prepareIdNumber) throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf.set("distributed.data.path", "target/test" + StateManagerInternalTest.class.getSimpleName());
    conf.setInt("file.cache.capacity", cacheCapacity);
    conf.setLong("distributed.data.delete.check.interval.time", 1);
    conf.setBoolean("distributed.data.format", true);
    conf.setInt("file.id.prepare.number", prepareIdNumber);
    return new StateManagerInternal(conf, null);
  }

  @Test
  public void upToCacheCapacity() throws Exception {
    stateManagerInternal = createStateManagerInternal(5, 1);
    File[] files0 = stateManagerInternal.insertFileByPath("/0", 0, -1, (byte) 1, false);
    File[] files1 = stateManagerInternal.insertFileByPath("/1", 0, -1, (byte) 1, false);
    File[] files2 = stateManagerInternal.insertFileByPath("/2", 0, -1, (byte) 1, false);
    File[] files3 = stateManagerInternal.insertFileByPath("/3", 0, -1, (byte) 1, false);
    File[] files4 = stateManagerInternal.insertFileByPath("/4", 0, -1, (byte) 1, false);

    assertThat(files0[0].path, is("/0"));
    assertThat(files1[0].path, is("/1"));
    assertThat(files2[0].path, is("/2"));
    assertThat(files3[0].path, is("/3"));
    assertThat(files4[0].path, is("/4"));
    if (stateManagerInternal != null) stateManagerInternal.close();
  }

  @Test
  public void updateFileToNewFolder() throws Exception {
    stateManagerInternal = createStateManagerInternal(100000, 10000);

    String srcPath0 = "/jiwan/folder0/file0";
    String srcPath1 = "/jiwan/folder1/file1";
    String srcPath11 = "/jiwan/folder1";
    File file11 = stateManagerInternal.findFileByPath(srcPath11);
    file11 = stateManagerInternal.insertFileByPath(srcPath11, 0, -1, (byte) 1, false)[1];
    File[] file11Children = stateManagerInternal.findFileChildrenByPath(srcPath11);
    stateManagerInternal.insertFileByPath(srcPath0, 0, 0, (byte) 0, true);
    stateManagerInternal.updateFileByPathAndPath(srcPath0, srcPath1);
    file11Children = stateManagerInternal.findFileChildrenByPath(srcPath11);
    assertThat(file11 != null, is(true));
    assertThat(file11Children != null, is(true));
    assertThat(file11Children.length, is(1));
    assertThat(file11Children[0].path, is(srcPath1));

    if (stateManagerInternal != null) stateManagerInternal.close();
  }
}
