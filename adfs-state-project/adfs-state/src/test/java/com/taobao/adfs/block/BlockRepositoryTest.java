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

package com.taobao.adfs.block;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockRepository;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.distributed.test.DistributedDataRepositoryBaseOnTableTest;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class BlockRepositoryTest extends DistributedDataRepositoryBaseOnTableTest {
  public static String simulatorDescription =
      "nn_state.block:id=integer|datanodeId=integer|numbytes=integer|generationStamp=integer|fileId=integer|fileIndex=integer|version=integer|operateIdentifier=string:PRIMARY=0,1|DATANODE_ID=1|FILE_ID=4|VERSION=6;";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
  }

  public Configuration createConf() {
    Configuration conf = new Configuration(false);
    conf.set("distributed.data.repository.class.name", BlockRepository.class.getName());
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", simulatorDescription);
    conf.setInt("block.cache.capacity", 1);
    conf.set("distributed.data.path", "target/test" + getClass().getSimpleName());
    conf.setLong("distributed.data.delete.check.interval.time", 1);
    // conf.set("database.executor.class.name", DatabaseExecutorForMysqlClient.class.getName());
    // conf.set("mysql.server.conf.mysqld.bind-address", "127.0.0.1");
    // conf.set("mysql.server.conf.mysqld.port", "40012");
    return conf;
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (repositories.get(BlockRepository.class) != null) repositories.get(BlockRepository.class).close();
  }

  public BlockRepository getRepository() throws Exception {
    return (BlockRepository) super.getRepository();
  }

  @Test
  public void updateWithExceptionForNotExisted() throws Exception {
    Throwable t = null;
    try {
      getRepository().update(new Block(), -1);
    } catch (Throwable throwable) {
      t = throwable;
    }
    assertThat(t instanceof IOException, is(true));
  }

  @Test
  public void deleteWithoutException() throws Exception {
    Block block = new Block();
    block.id = 1L;
    block.datanodeId = 1;
    block.fileId = 1;
    block.numbytes = 1L;
    block.generationStamp = 1L;
    block.fileIndex = 1;

    getRepository().insert(block, false);
    Block deletedBlock = (Block) getRepository().delete(block);
    assertThat(deletedBlock.version == -2, is(true));
    assertThat(getRepository().findByKeys(block.getKeys()) == null, is(true));
  }
}
