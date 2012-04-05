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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;


/**
 * This class tests that the DFS command mkdirs cannot create subdirectories
 * from a file when passed an illegal path.  HADOOP-281.
 */
public class TestMNDFSMkdirs extends TestCase {
  
  private Configuration conf = new Configuration();
  
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    DataOutputStream stm = fileSys.create(name);
    stm.writeBytes("wchien");
    stm.close();
  }
  
  /**
   * Tests mkdirs can create a directory that does not exist and will
   * not create a subdirectory off a file.
   */
  private void dfsMkdirs(boolean isConnetToDifferentNN) throws Exception {
    
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 2, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fileSys = null;
  
    if(cluster.getNameNodes().size() == 1) {
      fileSys = cluster.getFileSystem();
      } else {
        ArrayList<DataNode> dnlist=cluster.getDataNodes();
        ArrayList<NameNode> nnlist=cluster.getNameNodes();
        int nnindex = 0;  
        //get the namenode index which one datanode connects to      
        Random r = new Random();
        int dnindex = r.nextInt(dnlist.size());
        for(int i = 0;i < nnlist.size(); i++) {
          if(nnlist.get(i).getNameNodeAddress() == dnlist.get(dnindex).getNameNodeAddr()) {
            nnindex = i;
            break;
            }     
          }
        if(!isConnetToDifferentNN) {
          //1.client and dn connect to the same nn
          fileSys = cluster.getFileSystem(nnindex);
        } else {
          //2.connet to different nn
          while(true) {
            int j = r.nextInt(nnlist.size());
            if(j != nnindex) {
              nnindex = j;
              break;            
            }       
          }
          fileSys = cluster.getFileSystem(nnindex);
        }
      }

    try {
      mkDirs(fileSys);               
      } finally {
        fileSys.close();
        cluster.shutdown();
        }
   }


  private void mkDirs(FileSystem fileSys) throws IOException {
      // First create a new directory with mkdirs
      Path myPath = new Path("/test/mkdirs");
      assertTrue(fileSys.mkdirs(myPath));
      assertTrue(fileSys.exists(myPath));
      assertTrue(fileSys.mkdirs(myPath));

      // Second, create a file in that directory.
      Path myFile = new Path("/test/mkdirs/myFile");
      writeFile(fileSys, myFile);
        
           
      // Third, use mkdir to create a subdirectory off of that file,
      // and check that it fails.
      Path myIllegalPath = new Path("/test/mkdirs/myFile/subdir");
      Boolean exist = true;
      try {
        fileSys.mkdirs(myIllegalPath);
        } catch (IOException e) {
          exist = false;
        }
        assertFalse(exist);
        assertFalse(fileSys.exists(myIllegalPath));
        fileSys.delete(myFile, true);
        
  } 
  
  
  public void testDFSMultiNNMkdirsWithSameConnect() throws Exception {
    conf.set("dfs.namenode.port.list","0,0");
    dfsMkdirs(false);
    }
  
  public void testDFSMultiNNMkdirsWithDiffConnect() throws Exception {
    conf.set("dfs.namenode.port.list","0,0");
    dfsMkdirs(true);
    }
}