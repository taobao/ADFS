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

 package org.apache.hadoop.hdfs.server.datanode;

import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.AbsNameNodeSelector;
import org.apache.hadoop.hdfs.server.common.ConfNameNodeSelector;
import org.apache.hadoop.util.ReflectionUtils;

import junit.framework.TestCase;

public class TestSNConfNameNodeSelector extends TestCase {
	
  public void testNamenodeSelector() {
    Configuration conf = new Configuration();
    conf.setStrings("dfs.namenode.selector", 
        "org.apache.hadoop.hdfs.server.common.ConfNameNodeSelector");
    conf.setStrings("dfs.namenode.rpcaddr.list", "a,b,c");
    HashSet<String> nnSet = new HashSet<String>();
    nnSet.add("a");
    nnSet.add("b");
    nnSet.add("c");
    AbsNameNodeSelector namenodeSelector;
    namenodeSelector = (AbsNameNodeSelector) ReflectionUtils.newInstance(conf
        .getClass("dfs.namenode.selector", ConfNameNodeSelector.class,
            AbsNameNodeSelector.class), conf);

    namenodeSelector.refreshNameNodeList(conf);

    String sel = namenodeSelector.selectNextNameNodeAddress();
    assertTrue("selected: " + sel + " is contained", nnSet.contains(sel));;
  }
	

}
