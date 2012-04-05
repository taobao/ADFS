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

package com.taobao.adfs.benchmark;

import org.apache.hadoop.util.ProgramDriver;

public class ADFSTestDriver {
  
  /**
   * A description of the test program for running all the tests using jar file
   */
  public static void main(String argv[]){
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("create_write", CreateWrite.class, "test create and write operation");
      pgd.addClass("append_write", AppendWrite.class, "test append and write operation");
      pgd.addClass("open_read", OpenRead.class, "test open and read operation");
      pgd.addClass("re_open_read", ReOpenRead.class, "test open and read same file");
      pgd.addClass("rename_test", RenameTestPerformance.class, "test rename operation");
      pgd.addClass("delete_test", DeleteTestPerformance.class, "test delete operation");
      pgd.addClass("create", CreateOnly.class, "test only create operation");
      pgd.addClass("append", AppendOnly.class, "test only append operation");
      pgd.driver(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }
}

