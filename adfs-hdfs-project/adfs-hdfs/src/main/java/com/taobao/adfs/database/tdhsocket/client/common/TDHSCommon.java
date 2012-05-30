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

package com.taobao.adfs.database.tdhsocket.client.common;

import com.taobao.adfs.database.tdhsocket.client.protocol.TDHSProtocol;
import com.taobao.adfs.database.tdhsocket.client.protocol.TDHSProtocolBinary;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-11-1 上午10:51
 */
public final class TDHSCommon {

    public static final int REQUEST_MAX_FIELD_NUM = 256;

    public static final int REQUEST_MAX_KEY_NUM = 10;

    public static final TDHSProtocol PROTOCOL_FOR_BINARY = new TDHSProtocolBinary();

    public enum RequestType {
        GET(0), UPDATE(10), DELETE(11), INSERT(12), BATCH(20), SHAKE_HAND(0xFFFF);

        private int value;

        RequestType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

    public enum FindFlag {
        TDHS_EQ(0), TDHS_GE(1), TDHS_LE(2), TDHS_GT(3), TDHS_LT(4), TDHS_IN(5), TDHS_DEQ(6);

        private int value;

        FindFlag(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

    public enum FilterFlag {
        TDHS_EQ(0), TDHS_GE(1), TDHS_LE(2), TDHS_GT(3), TDHS_LT(4), TDHS_NOT(5);

        private int value;

        FilterFlag(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

    public enum UpdateFlag {
        TDHS_UPDATE_SET(0), TDHS_UPDATE_ADD(1), TDHS_UPDATE_SUB(2), TDHS_UPDATE_NOW(3);

        private int value;

        UpdateFlag(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }


}
