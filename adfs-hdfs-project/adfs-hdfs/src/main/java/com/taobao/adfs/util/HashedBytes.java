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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class encapsulates a byte array and overrides hashCode and equals so
 * that it's identity is based on the data rather than the array instance.
 */
public class HashedBytes {
  private static final Log LOG = LogFactory.getLog(HashedBytes.class);

  private final byte[] bytes;

  private final int hashCode;

  public HashedBytes(byte[] bytes) {
    this.bytes = bytes;
    hashCode = WritableComparator.hashBytes(bytes, bytes.length);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    HashedBytes other = (HashedBytes) obj;
    return Arrays.equals(bytes, other.bytes);
  }

  @Override
  public String toString() {
    if (bytes == null) { return "null"; }
    return toStringBinary(bytes, 0, bytes.length);
  }

  /**
   * Write a printable representation of a byte array. Non-printable
   * characters are hex escaped in the format \\x%02X, eg: \x00 \x05 etc
   * 
   * @param b
   *          array to write out
   * @param off
   *          offset to start at
   * @param len
   *          length to write
   * @return string output
   */
  public static String toStringBinary(final byte[] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    try {
      String first = new String(b, off, len, "ISO-8859-1");
      for (int i = 0; i < first.length(); ++i) {
        int ch = first.charAt(i) & 0xFF;
        if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
            || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0) {
          result.append(first.charAt(i));
        } else {
          result.append(String.format("\\x%02X", ch));
        }
      }
    } catch (UnsupportedEncodingException e) {
      LOG.error("ISO-8859-1 not supported?", e);
    }
    return result.toString();
  }
}
