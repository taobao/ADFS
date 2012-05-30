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

package com.taobao.adfs.distributed.test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.taobao.adfs.database.DatabaseExecutorForHandlerSocket;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedException;

public abstract class DistributedDataRepositoryBaseOnTableTest {
  abstract public Configuration createConf();

  protected static Map<Class<?>, DistributedDataRepositoryBaseOnTable> repositories =
      new HashMap<Class<?>, DistributedDataRepositoryBaseOnTable>();

  @Before
  public void setupBeforeTest() throws Exception {
    getRepository().getVersion().set(0);
    getRepository().format();
  }

  public DistributedDataRepositoryBaseOnTable getRepository() throws Exception {
    if (repositories.get(getClass()) != null) return repositories.get(getClass());
    Configuration conf = createConf();
    Class<?> repositoryClassName = Class.forName(conf.get("distributed.data.repository.class.name"));
    Constructor<?> repositoryConstructor = repositoryClassName.getConstructor(Configuration.class);
    repositories.put(getClass(), (DistributedDataRepositoryBaseOnTable) repositoryConstructor.newInstance(conf));
    // TODO:
    repositories.get(getClass()).open(null);
    repositories.get(getClass()).format();
    return repositories.get(getClass());
  }

  @Test
  public void getDistributedExceptionForHsClientThrowException() throws Exception {
    if (!DatabaseExecutorForHandlerSocket.class.getName().equals(
        getRepository().conf.get("database.executor.class.name"))) return;
    // checkThread/deleteThread will do find operation, so close it firstly
    getRepository().checkThread.close();
    getRepository().conf.set("database.executor.handlersocket.simulator.throw.exception.for.find",
        TimeoutException.class.getName());
    try {
      getRepository().findByVersion(0);
      throw new IOException();
    } catch (Throwable t) {
      assertThat(DistributedException.isNeedRestore(t), is(true));
    }
  }
}
