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

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.AbsNameNodeSelector;
import org.apache.hadoop.hdfs.server.common.ZkNamenodeSelector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This testcase is for ZkNamenodeSelector. 
 *
 */
public class TestMNZkNamenodeSelector extends TestCase {
  
  static class ZkUtil {
    public static final String SLASH = "/";
   
    /**
     * Recursively delete all the contents under path in zookeeper configured in conf. 
     * @param conf The Configuration object used to find zookeeper. 
     * @param path The path to clean. 
     * @param deleteSelf If true, delete the path as well. 
     * @throws IOException
     */
    public static void cleanZkPath(Configuration conf, String path, boolean deleteSelf) throws IOException {
      List<String> children = ZKClient.getInstance(conf).getChildren(path, null);
      if (children != null) {
        for (String child: children){
          cleanZkPath(conf, path + SLASH + child, true);
        }
      }
      if (deleteSelf)
        ZKClient.getInstance(conf).delete(path, false, true);
    }
    
    /**
     * Add Strings under path as children nodes on the zookeeper configured in conf. 
     * @param conf The Configuration object used to find zookeeper. 
     * @param path The parent path under which to add these Strings. 
     * @param children The set of Strings to be added. 
     * @throws IOException
     */
    public static void addChildrenToZkPath(Configuration conf, String path, HashSet<String> children) throws IOException{
      if (ZKClient.getInstance(conf).exist(path) == null){
        ZKClient.getInstance(conf).create(path, new byte[0], true, true);
      }
      for (String child : children){
        ZKClient.getInstance(conf).create(path + SLASH + child, new byte[0], true, true);
      }
    }
  }
  
  /**
   * It first cleans the path on zookeeper, adds a set of ip addresses to the path, 
   * and then uses ZkNamenodeSelector to select twice. These two Strings should be all
   * contained in the ip set, and should also be different from each other. 
   * @throws IOException
   */
  public void testNamenodeSelector() throws IOException {
    Configuration conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    conf.setStrings("dfs.namenode.selector", 
        "org.apache.hadoop.hdfs.server.common.ZkNamenodeSelector");
    System.out.println("HouSong's debug: " + conf.get("zookeeper.server.list"));
    HashSet<String> nnSet = new HashSet<String>();
    nnSet.add("1.2.3.4:100");
    nnSet.add("2.3.4.5:200");
    nnSet.add("3.4.5.6:300");
    ZkUtil.cleanZkPath(conf, FSConstants.ZOOKEEPER_NAMENODE_GROUP, false);
    ZkUtil.addChildrenToZkPath(conf, FSConstants.ZOOKEEPER_NAMENODE_GROUP, nnSet);
    
    AbsNameNodeSelector namenodeSelector;
    namenodeSelector = (AbsNameNodeSelector) ReflectionUtils.newInstance(conf
        .getClass("dfs.namenode.selector", ZkNamenodeSelector.class,
            AbsNameNodeSelector.class), conf);
    namenodeSelector.refreshNameNodeList(conf);
    
    String sel = namenodeSelector.selectNextNameNodeAddress();
    assertTrue("selected: " + sel + " should be contained", nnSet.contains(sel));
    nnSet.remove(sel);
    
    String sel2 = namenodeSelector.selectNextNameNodeAddress();
    assertTrue("selected: " + sel2 + " should be contained", nnSet.contains(sel2) && 
        !sel2.equals(sel));
    assertFalse("selected: " + sel2 + " was selected multiple times", sel2.equals(sel));
    
    ZkUtil.cleanZkPath(conf, FSConstants.ZOOKEEPER_NAMENODE_GROUP, false);
  }
}
