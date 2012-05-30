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

package com.taobao.adfs.database.tdhsocket.client.util;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-1-12 下午12:31
 */
final public class ByteOrderUtil {

    public static void writeIntToNet(byte[] bytes, int i, long value) {
        if (i > (bytes.length - 4)) {
            throw new IllegalArgumentException("out of range");
        }
        for (int j = 3; j >= 0; i++, j--) {
            bytes[i] = (byte) ((value >>> (8 * j)) & 0XFF);
        }
    }


    public static long getUnsignInt(final byte data[], final int pos) {
        int v = (data[pos] & 0xff) << 24 |
                (data[pos + 1] & 0xff) << 16 |
                (data[pos + 2] & 0xff) << 8 |
                (data[pos + 3] & 0xff) << 0;
        return v & 0xFFFFFFFFL;
    }
}
