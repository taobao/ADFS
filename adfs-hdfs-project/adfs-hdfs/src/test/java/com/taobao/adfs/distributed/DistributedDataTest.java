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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.distributed.example.ExampleData;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedDataTest {
  static ExampleData exampleData = null;

  @BeforeClass
  static public void setupAfterClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
    Configuration conf = new Configuration(false);
    conf.set("distributed.data.path", "target/test" + DistributedDataTest.class.getSimpleName());
    conf.setLong("distributed.data.delete.check.interval.time", 1);
    conf.set("distributed.data.format", "true");
    exampleData = new ExampleData(conf);
    exampleData.format();
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (exampleData != null) exampleData.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    exampleData.format();
  }

  @Test
  public void format() throws Exception {
    exampleData.write("jiwan@taobao.com");
    assertThat(exampleData.getDataVersion() == 1, is(true));
    assertThat(exampleData.read().equals("jiwan@taobao.com"), is(true));
    exampleData.format();
    assertThat(exampleData.getDataVersion() == 0, is(true));
    assertThat(exampleData.read().isEmpty(), is(true));
  }
}
