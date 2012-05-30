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
  static Map<String, Integer> hostnameToIpValueMapCache = new HashMap<String, Integer>();
  static Map<Integer, String> ipValueToIpStringMapCache = new HashMap<Integer, String>();
  static Field addressFieldForInet4Address = null;

  static public int getAddress(String hostname) throws IOException {
    try {
      hostname = hostname.trim();
      Integer ipValue = hostnameToIpValueMapCache.get(hostname);
      if (ipValue != null) return ipValue;

      try {
        // think address as a IP string
        ipValue = getAddressByIpString(hostname);
        synchronized (hostnameToIpValueMapCache) {
          hostnameToIpValueMapCache.put(hostname, ipValue);
        }
        if (ipValueToIpStringMapCache.get(ipValue) == null) {
          synchronized (ipValueToIpStringMapCache) {
            ipValueToIpStringMapCache.put(ipValue, hostname);
          }
        }
        return ipValue;
      } catch (Throwable t) {
        // ignore this exception
      }

      // parse address to IP string
      if (addressFieldForInet4Address == null)
        (addressFieldForInet4Address = InetAddress.class.getDeclaredField("address")).setAccessible(true);

      InetAddress inetAddress = Inet4Address.getByName(hostname);
      ipValue = (Integer) addressFieldForInet4Address.get(inetAddress);
      synchronized (hostnameToIpValueMapCache) {
        hostnameToIpValueMapCache.put(hostname, ipValue);
      }
      return ipValue;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  static public String getAddress(int ipValue) {
    String ipString = ipValueToIpStringMapCache.get(ipValue);
    if (ipString != null) return ipString;
    int[] addr = new int[4];
    addr[0] = (int) ((ipValue >>> 24) & 0xFF);
    addr[1] = (int) ((ipValue >>> 16) & 0xFF);
    addr[2] = (int) ((ipValue >>> 8) & 0xFF);
    addr[3] = (int) (ipValue & 0xFF);
    StringBuilder stringBuilder = new StringBuilder(16);
    stringBuilder.append(addr[0]).append('.');
    stringBuilder.append(addr[1]).append('.');
    stringBuilder.append(addr[2]).append('.');
    stringBuilder.append(addr[3]);
    ipString = stringBuilder.toString();
    synchronized (ipValueToIpStringMapCache) {
      ipValueToIpStringMapCache.put(ipValue, ipString);
    }
    if (hostnameToIpValueMapCache.get(ipString) == null) {
      synchronized (hostnameToIpValueMapCache) {
        hostnameToIpValueMapCache.put(ipString, ipValue);
      }
    }
    return ipString;
  }

  static private int getAddressByIpString(String ipString) throws IOException {
    if (ipString == null) throw new IOException("address is null");
    String[] addressFields = ipString.split("\\.", 4);
    if (addressFields.length != 4 || addressFields[0].isEmpty() || addressFields[1].isEmpty()
        || addressFields[2].isEmpty() || addressFields[3].isEmpty())
      throw new IOException("invalid ip address=" + ipString);
    int addressValue = Integer.valueOf(addressFields[0]) << 24;
    addressValue = Integer.valueOf(addressFields[1]) << 16 | addressValue;
    addressValue = Integer.valueOf(addressFields[2]) << 8 | addressValue;
    addressValue = Integer.valueOf(addressFields[3]) | addressValue;
    return addressValue;
  }

  public static int getIp(long id) {
    return (int) (id >>> 32);
  }

  public static int getPort(long id) {
    return (int) (id & 0xFFFFFFFF);
  }

  public static String getIpAndPort(long id) {
    return getAddress(getIp(id)) + ":" + getPort(id);
  }
}
