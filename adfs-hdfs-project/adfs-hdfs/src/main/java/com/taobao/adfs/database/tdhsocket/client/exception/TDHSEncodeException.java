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

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-2 下午1:28
 */
public class TDHSEncodeException extends TDHSException {

    private static final long serialVersionUID = 4248787784350507825L;

    public TDHSEncodeException() {
    }

    public TDHSEncodeException(String message) {
        super(message);
    }

    public TDHSEncodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TDHSEncodeException(Throwable cause) {
        super(cause);
    }

}
