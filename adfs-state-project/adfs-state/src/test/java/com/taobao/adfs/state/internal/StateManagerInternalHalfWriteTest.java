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

package com.taobao.adfs.state.internal;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.distributed.DistributedDataCache;
import com.taobao.adfs.distributed.DistributedException;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepositoryTest;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerInternalHalfWriteTest {
  protected static StateManagerInternal stateManagerInternal = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
    Utilities.setLoggerLevel(DistributedDataCache.class.getName(), Level.DEBUG.toString(), null);
    Configuration conf = new Configuration(false);
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf.setInt("file.cache.capacity", 10000);
    conf.set("distributed.data.path", "target/test" + StateManagerInternalTest.class.getSimpleName());
    stateManagerInternal = new StateManagerInternal(conf, null);
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (stateManagerInternal != null) stateManagerInternal.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    stateManagerInternal.format();
    StateManagerInternal.flagForHalfWriteTest = false;
  }

  @Test
  public void testInsertFileByPath() throws Exception {
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.insertFileByPath("/a/b", 0, 0, (byte) 0, true);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(((Object[]) halfResult)[0].equals(stateManagerInternal.findFileByPath("/a")), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/b") == null, is(true));
  }

  @Test
  public void testDeleteFileAndBlockByPath() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true);
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    block.fileIndex = 0;
    block.fileId = stateManagerInternal.findFileByPath("/a").id;
    stateManagerInternal.insertBlockByBlock(block);
    block.id = 1;
    block.datanodeId = 1;
    block.fileIndex = 1;
    stateManagerInternal.insertBlockByBlock(block);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.deleteFileAndBlockByPath("/a", true);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 2, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal.findBlockByFileId(block.fileId).length == 1, is(true));
    assertThat(((File) ((Object[]) halfResult)[0]).id == block.fileId, is(true));
    assertThat(((Block) ((Object[]) halfResult)[1]).fileId == block.fileId, is(true));
  }

  @Test
  public void testUpdateFileByPathAndPath() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.updateFileByPathAndPath("/a", "/A/B/C");
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(((Object[]) halfResult)[0].equals(stateManagerInternal.findFileByPath("/A")), is(true));
    assertThat(stateManagerInternal.findFileByPath("/A/B") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/A/B/C") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a") != null, is(true));
  }

  @Test
  public void testDeleteFileByPath() throws Exception {
    stateManagerInternal.insertFileByPath("/a/b", 0, 0, (byte) 0, true);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.deleteFileByPath("/a", true);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a") != null, is(true));
  }

  @Test
  public void testDeleteFileByFiles() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true);
    stateManagerInternal.insertFileByPath("/b", 0, 0, (byte) 0, true);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.deleteFileByFiles(
          new File[] { stateManagerInternal.findFileByPath("/a"), stateManagerInternal.findFileByPath("/b") }, true);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b") != null, is(true));
  }

  @Test
  public void testDeleteBlockById() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternal.insertBlockByBlock(block);
    block.id = 0;
    block.datanodeId = 1;
    stateManagerInternal.insertBlockByBlock(block);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.deleteBlockById(0);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(stateManagerInternal.findBlockById(0).length == 1, is(true));
  }

  @Test
  public void testDeleteBlockByFileId() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    block.fileId = 0;
    block.fileIndex = 0;
    stateManagerInternal.insertBlockByBlock(block);
    block.id = 1;
    block.datanodeId = 1;
    block.fileIndex = 1;
    stateManagerInternal.insertBlockByBlock(block);
    StateManagerInternal.flagForHalfWriteTest = true;
    Object halfResult = null;
    try {
      stateManagerInternal.deleteBlockByFileId(0);
    } catch (Throwable t) {
      halfResult = DistributedException.getDistributedExceptionResult(t);
    }
    assertThat(halfResult != null, is(true));
    assertThat(((Object[]) halfResult).length == 1, is(true));
    assertThat(stateManagerInternal.findBlockByFileId(0).length == 1, is(true));
  }
}
