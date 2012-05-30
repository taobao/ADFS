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

package com.taobao.adfs.database.tdhsocket.client.response;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-11-1 上午11:19
 */
public class TDHSResponseEnum {

    public enum ClientStatus {
        OK(200), ACCEPT(202), MULTI_STATUS(207), BAD_REQUEST(400), FORBIDDEN(403), NOT_FOUND(404), REQUEST_TIME_OUT(
                408), SERVER_ERROR(500), NOT_IMPLEMENTED(501), DB_ERROR(502), SERVICE_UNAVAILABLE(503), UNKNOW(-1);


        private int status;

        ClientStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public static ClientStatus valueOf(int status) {
            for (ClientStatus s : ClientStatus.values()) {
                if (status == s.getStatus()) {
                    return s;
                }
            }
            ClientStatus unknow = UNKNOW;
            unknow.setStatus(status);
            return unknow;
        }
    }

    public enum ErrorCode {
        CLIENT_ERROR_CODE_FAILED_TO_OPEN_TABLE(1, "TDH_SOCKET failed to open table!"),
        CLIENT_ERROR_CODE_FAILED_TO_OPEN_INDEX(2, "TDH_SOCKET failed to open index!"),
        CLIENT_ERROR_CODE_FAILED_TO_MISSING_FIELD(3, "TDH_SOCKET field is missing!"),
        CLIENT_ERROR_CODE_FAILED_TO_MATCH_KEY_NUM(4, "TDH_SOCKET request can't match the key number!"),
        CLIENT_ERROR_CODE_FAILED_TO_LOCK_TABLE(5, "TDH_SOCKET failed to lock table!"),
        CLIENT_ERROR_CODE_NOT_ENOUGH_MEMORY(6, "TDH_SOCKET server don't have enough memory!"),
        CLIENT_ERROR_CODE_DECODE_REQUEST_FAILED(7, "TDH_SOCKET server can't decode your request!"),
        CLIENT_ERROR_CODE_FAILED_TO_MISSING_FIELD_IN_FILTER_OR_USE_BLOB(8,
                "TDH_SOCKET field is missing in filter or use blob!"),
        CLIENT_ERROR_CODE_FAILED_TO_COMMIT(9, "TDH_SOCKET failed to commit, will be rollback!"),
        CLIENT_ERROR_CODE_NOT_IMPLEMENTED(10, "TDH_SOCKET not implemented!"),
        CLIENT_ERROR_CODE_REQUEST_TIME_OUT(11, "TDH_SOCKET request time out!"),
        CLIENT_ERROR_CODE_UNAUTHENTICATION(12, "TDH_SOCKET request is unauthentication!"),
        CLIENT_ERROR_CODE_KILLED(13, "TDH_SOCKET request is killed!"),
        CLIENT_ERROR_CODE_THROTTLED(14, "TDH_SOCKET request is throttled!"),
        CLIENT_ERROR_CODE_UNKNOW(-1, "TDH_SOCKET unknow error code!");

        private int code;

        private String errorMsg;

        ErrorCode(int code, String errorMsg) {
            this.code = code;
            this.errorMsg = errorMsg;
        }

        public int getCode() {
            return code;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public static ErrorCode valueOf(int code) {
            for (ErrorCode c : ErrorCode.values()) {
                if (code == c.getCode()) {
                    return c;
                }
            }
            ErrorCode clientErrorCodeUnknow = CLIENT_ERROR_CODE_UNKNOW;
            clientErrorCodeUnknow.setCode(code);
            return clientErrorCodeUnknow;
        }


        @Override public String toString() {
            return "ErrorCode{" +
                    "code=" + code +
                    ", errorMsg='" + errorMsg + '\'' +
                    '}';
        }
    }

    public enum FieldType {

        MYSQL_TYPE_DECIMAL(0),
        MYSQL_TYPE_TINY(1),
        MYSQL_TYPE_SHORT(2),
        MYSQL_TYPE_LONG(3),
        MYSQL_TYPE_FLOAT(4),
        MYSQL_TYPE_DOUBLE(5),
        MYSQL_TYPE_NULL(6),
        MYSQL_TYPE_TIMESTAMP(7),
        MYSQL_TYPE_LONGLONG(8),
        MYSQL_TYPE_INT24(9),
        MYSQL_TYPE_DATE(10),
        MYSQL_TYPE_TIME(11),
        MYSQL_TYPE_DATETIME(12),
        MYSQL_TYPE_YEAR(13),
        MYSQL_TYPE_NEWDATE(14),
        MYSQL_TYPE_VARCHAR(15),
        MYSQL_TYPE_BIT(16),
        MYSQL_TYPE_NEWDECIMAL(246),
        MYSQL_TYPE_ENUM(247),
        MYSQL_TYPE_SET(248),
        MYSQL_TYPE_TINY_BLOB(249),
        MYSQL_TYPE_MEDIUM_BLOB(25),
        MYSQL_TYPE_LONG_BLOB(251),
        MYSQL_TYPE_BLOB(252),
        MYSQL_TYPE_VAR_STRING(253),
        MYSQL_TYPE_STRING(254),
        MYSQL_TYPE_GEOMETRY(255);

        private int type;

        private static FieldType[] cached_type = new FieldType[256];

        FieldType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static FieldType valueOf(int type) {
            if (type > 255) {
                return null;
            }
            if (cached_type[type] != null) {
                return cached_type[type];
            }
            for (FieldType t : FieldType.values()) {
                cached_type[type] = t;
                if (type == t.getType()) {
                    return t;
                }
            }
            return null;
        }
    }
}
