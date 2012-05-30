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

import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.util.ByteOrderUtil;

import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-11-1 上午11:16
 */
public class TDHSResponse {

    private TDHSResponseEnum.ClientStatus status;

    private TDHSResponseEnum.ErrorCode errorCode;

    private int dbErrorCode;

    private int fieldNumber;

    private List<String> fieldNames;

    private List<TDHSResponseEnum.FieldType> fieldTypes;

    private List<List<String>> fieldData;

    private byte data[];

    private String charestName;

    private boolean isParsed = false;


    public TDHSResponse(TDHSResponseEnum.ClientStatus status, List<String> fieldNames, byte[] data,
                        String charestName) {
        this.status = status;
        this.fieldNames = fieldNames;
        this.data = data;
        if (StringUtils.isNotBlank(charestName)) {
            this.charestName = charestName;
        }
    }

    private synchronized void parse() throws TDHSException {
        if (isParsed) {
            return;
        }
        if (400 <= status.getStatus() && 600 > status.getStatus()) {
            parseFailed(this.data);
        } else if (TDHSResponseEnum.ClientStatus.OK.equals(status)) {
            try {
                parseData(this.data);
            } catch (UnsupportedEncodingException e) {
                throw new TDHSException(e);
            }
        } else {
            throw new TDHSException("unknown response code!");
        }
        this.data = null;
        isParsed = true;
    }

    private void parseFailed(final byte data[]) {
        int code = (int) ByteOrderUtil.getUnsignInt(data, 0);
        if (status == TDHSResponseEnum.ClientStatus.DB_ERROR) {
            dbErrorCode = code;
        } else {
            errorCode = TDHSResponseEnum.ErrorCode.valueOf(code);
        }
    }

    private void parseData(final byte data[]) throws UnsupportedEncodingException {
        int len = data.length;
        int pos = 0;
        fieldData = new ArrayList<List<String>>();
        //read field number
        fieldNumber = (int) ByteOrderUtil.getUnsignInt(data, pos);
        pos += 4;
        fieldTypes = new ArrayList<TDHSResponseEnum.FieldType>(fieldNumber);
        for (int i = 0; i < fieldNumber; i++) {
            fieldTypes.add(TDHSResponseEnum.FieldType.valueOf(data[pos] & 0xFF));
            pos++;
        }
        while (pos < len) {
            List<String> record = new ArrayList<String>(fieldNumber);
            for (int i = 0; i < fieldNumber; i++) {
                int fieldLength = (int) ByteOrderUtil.getUnsignInt(data, pos);
                pos += 4;
                if (fieldLength > 0) {
                    if (fieldLength == 1 && data[pos] == 0) {
                        record.add("");
                    } else {
                        byte f[] = new byte[fieldLength];
                        System.arraycopy(data, pos, f, 0, fieldLength);
                        record.add(
                                StringUtils.isNotBlank(charestName) ? new String(f, this.charestName) : new String(f));
                    }
                    pos += fieldLength;
                } else {
                    record.add(null);
                }
            }
            fieldData.add(record);
        }
    }

    public String getCharestName() {
        return charestName;
    }

    public void setCharestName(String charestName) {
        if (StringUtils.isNotBlank(charestName)) {
            this.charestName = charestName;
        }
    }


    public TDHSResponseEnum.ClientStatus getStatus() {
        return status;
    }

    public TDHSResponseEnum.ErrorCode getErrorCode() throws TDHSException {
        parse();
        return errorCode;
    }

    public int getFieldNumber() throws TDHSException {
        parse();
        return fieldNumber;
    }

    public List<TDHSResponseEnum.FieldType> getFieldTypes() throws TDHSException {
        parse();
        return fieldTypes;
    }

    public List<List<String>> getFieldData() throws TDHSException {
        parse();
        return fieldData;
    }

    public int getDbErrorCode() throws TDHSException {
        parse();
        return dbErrorCode;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public ResultSet getResultSet() throws TDHSException {
        if (TDHSResponseEnum.ClientStatus.OK.equals(status)) {
            return new TDHSResutSet(getFieldNames(), getFieldTypes(), getFieldData());
        }
        return null;
    }

    public ResultSet getResultSet(List<String> alias) throws TDHSException {
        if (TDHSResponseEnum.ClientStatus.OK.equals(status)) {
            if (alias == null || alias.size() != getFieldNames().size()) {
                throw new TDHSException("alias is wrong! alias:[" + alias + "] fieldNames:[" + getFieldNames() + "]");
            }
            return new TDHSResutSet(alias, getFieldTypes(), getFieldData());
        }
        return null;
    }

    @Override public String toString() {
        try {
            return "TDHSResponse{" +
                    "status=" + getStatus() +
                    ", errorCode=" + getErrorCode() +
                    ", dbErrorCode=" + getDbErrorCode() +
                    ", fieldNumber=" + getFieldNumber() +
                    ", fieldTypes=" + getFieldTypes() +
                    ", fieldData=" + getFieldData() +
                    '}';
        } catch (TDHSException e) {
            PrintWriter pw = new PrintWriter(new StringWriter());
            e.printStackTrace(pw);
            return "TDHSResponse parse failed!\n" + pw.toString();
        }
    }


    public String getErrorMessage() {
        try {
            return "status=" + getStatus() +
                    ", errorCode=" + getErrorCode() +
                    ", dbErrorCode=" + getDbErrorCode();
        } catch (TDHSException e) {
            PrintWriter pw = new PrintWriter(new StringWriter());
            e.printStackTrace(pw);
            return "TDHSResponse parse failed!\n" + pw.toString();
        }
    }
}
