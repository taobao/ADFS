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

package com.taobao.adfs.database.tdhsocket.client.exception;

import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-2-21 下午5:07
 */
public class TDHSBatchException extends TDHSException {
    private TDHSResponse response;

    public TDHSBatchException(TDHSResponse response) {
        this.response = response;
    }

    public TDHSResponse getResponse() {
        return response;
    }

    @Override public String toString() {
        return "TDHSBatchException{" +
                "response=" + response +
                '}';
    }
}
