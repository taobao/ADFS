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

package com.taobao.adfs.database.innodb;

import java.sql.ResultSet;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class InnodbJniClient {
  static {
    System.loadLibrary("innodb");
  }

  public native int open();

  public native int close();

  public native int insert(String database, String table, Object[] value);

  public native ResultSet find(String database, String table, String index, Object[] key, int operator, int limit);

  public native int delete(String database, String table, String index, Object[] key, int operator, int limit);

  public native int update(String database, String table, String index, Object[] key, int operator, int limit,
      Object[] value);
}
