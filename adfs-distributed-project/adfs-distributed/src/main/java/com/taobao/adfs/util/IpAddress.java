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

package com.taobao.adfs.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class IpAddress {
  static Map<String, Integer> ipMapCache = new HashMap<String, Integer>();
  static Field addressFieldForInet4Address = null;

  static public int getAddress(String address) throws IOException {
    try {
      address = address.trim();
      Integer ipValue = ipMapCache.get(address);
      if (ipValue != null) return ipValue;

      try {
        ipMapCache.put(address, ipValue = getAddressByIpString(address));
        return ipValue;
      } catch (Throwable t) {
        // ignore this exception
      }

      if (addressFieldForInet4Address == null)
        (addressFieldForInet4Address = InetAddress.class.getDeclaredField("address")).setAccessible(true);

      InetAddress inetAddress = Inet4Address.getByName(address);
      ipMapCache.put(address, ipValue = (Integer) addressFieldForInet4Address.get(inetAddress));
      return ipValue;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  static public String getAddress(int address) {
    int[] addr = new int[4];
    addr[0] = (int) ((address >>> 24) & 0xFF);
    addr[1] = (int) ((address >>> 16) & 0xFF);
    addr[2] = (int) ((address >>> 8) & 0xFF);
    addr[3] = (int) (address & 0xFF);
    StringBuilder stringBuilder = new StringBuilder(16);
    stringBuilder.append(addr[0]).append('.');
    stringBuilder.append(addr[1]).append('.');
    stringBuilder.append(addr[2]).append('.');
    stringBuilder.append(addr[3]);
    return stringBuilder.toString();
  }

  static public int getAddressByIpString(String address) throws IOException {
    if (address == null) throw new IOException("address is null");
    String[] addressFields = address.split("\\.", 4);
    if (addressFields.length != 4 || addressFields[0].isEmpty() || addressFields[1].isEmpty()
        || addressFields[2].isEmpty() || addressFields[3].isEmpty())
      throw new IOException("invalid ip address=" + address);
    int addressValue = Integer.valueOf(addressFields[0]) << 24;
    addressValue = Integer.valueOf(addressFields[1]) << 16 | addressValue;
    addressValue = Integer.valueOf(addressFields[2]) << 8 | addressValue;
    addressValue = Integer.valueOf(addressFields[3]) | addressValue;
    return addressValue;
  }
}
