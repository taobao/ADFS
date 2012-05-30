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

package com.taobao.adfs.iosimulator.scenarios;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.command.AbstractCommand;
import com.taobao.adfs.iosimulator.command.DatanodeRegCommand;
import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.TDHExecutor;

public class DatanodeReg {
  protected int datanodeNum = 100;
  protected Logger logger = Logger.getLogger(getClass().getName());
  protected IExecutor executor;
  
  protected void init() {
    try {
      executor = new TDHExecutor();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
  
  public void setDatanodeNum(int dnum) {
    datanodeNum = dnum;
  }
  
  public int getDatanodeNum() {
    return datanodeNum;
  }
  
  protected void runTest() {
    System.out.println("Start Tests of " + this.getClass().getSimpleName());
    Long start = System.currentTimeMillis();
    try {
      for(int i = 0; i < datanodeNum; i++) {
        AbstractCommand cmd = new DatanodeRegCommand(executor, i);
        cmd.execute();
      }
    } finally {
      System.out.println(datanodeNum + 
          " datanodes created, elasped=" + (System.currentTimeMillis()-start) + "(milsec)");
      executor.close();
    }
  }
  
  public static void main(String[] args) {
    DatanodeReg dr = new DatanodeReg();
    dr.init();
    dr.runTest();
  }
}
