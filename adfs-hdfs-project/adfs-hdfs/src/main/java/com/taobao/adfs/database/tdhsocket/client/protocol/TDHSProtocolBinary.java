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

package com.taobao.adfs.database.tdhsocket.client.protocol;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSEncodeException;
import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;
import com.taobao.adfs.database.tdhsocket.client.request.Request;
import com.taobao.adfs.database.tdhsocket.client.request.RequestWithCharest;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-2 上午11:17
 */
public class TDHSProtocolBinary implements TDHSProtocol {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public BasePacket shakeHandPacket(int timeOut, @Nullable String readCode,
                                      @Nullable String writeCode) {

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(12 + 2 + 200);
            out.write("TDHS".getBytes());
            writeInt32ToStream(1, out);//版本号
            writeInt32ToStream(timeOut, out);//超时时间
            writeToStream(readCode, out, null); //read code
            writeToStream(writeCode, out, null);//write code
            return new BasePacket(TDHSCommon.RequestType.SHAKE_HAND, 0, out.toByteArray());
        } catch (IOException e) {
            assert false; //不应该发生
            logger.error("shakeHandPacket failed!", e);
            return null;
        }
    }

    public byte[] encode(RequestWithCharest o) throws TDHSEncodeException {
        try {
            o.isVaild();
            ByteArrayOutputStream out = new ByteArrayOutputStream(2 * 1024);
            encodeRequest(o, out, o.getCharestName());
            return out.toByteArray();
        } catch (IllegalAccessException e) {
            throw new TDHSEncodeException(e);
        } catch (IOException e) {
            throw new TDHSEncodeException(e);
        }
    }

    private static void encodeRequest(Request o, ByteArrayOutputStream out, String charestName)
            throws IllegalAccessException, IOException, TDHSEncodeException {
        for (Field f : o.getClass().getDeclaredFields()) {
            if (!Modifier.isPublic(f.getModifiers())) {
                f.setAccessible(true);
            }
            Object v = f.get(o);
            if (v instanceof Request) {
                encodeRequest((Request) v, out, charestName);
            } else if (f.getName().startsWith("_")) {
                writeObjectToStream(v, out, f.getName(), charestName);
            }
        }
    }

    private static void writeObjectToStream(Object o, ByteArrayOutputStream out, String fieldName, String charestName)
            throws IOException,
            TDHSEncodeException, IllegalAccessException {
        if (o == null || o instanceof String) {
            writeToStream((String) o, out, charestName);
        } else if (o instanceof Integer) {
            if (fieldName.startsWith("____")) {
                writeInt8ToStream((Integer) o, out);
            } else {
                writeInt32ToStream((Integer) o, out);
            }
        } else if (o instanceof Collection) {
            writeToStream(((Collection) o).toArray(), out, charestName);
        } else if (o instanceof Object[]) {
            writeToStream((Object[]) o, out, charestName);
        } else {
            throw new TDHSEncodeException("unkown type!");
        }
    }


    private static void writeToStream(String v, ByteArrayOutputStream out, @Nullable String charestName)
            throws IOException {
        if (v == null) {
            writeInt32ToStream(0, out);
        } else {
            byte[] sb = StringUtils.isNotBlank(charestName) ? v.toLowerCase().getBytes(charestName) :
                    v.toLowerCase().getBytes();
            writeInt32ToStream(sb.length + 1, out);
            out.write(sb);
            out.write(0);
        }
    }


    private static void writeToStream(Object[] array, ByteArrayOutputStream out, String charestName) throws IOException,
            TDHSEncodeException, IllegalAccessException {
        writeInt32ToStream(array.length, out);
        for (Object o : array) {
            if (o == null || o instanceof String) {
                writeToStream((String) o, out, charestName);
            } else if (o instanceof Integer) {
                writeInt32ToStream((Integer) o, out);
            } else if (o instanceof Request) {
                encodeRequest((Request) o, out, charestName);
            } else if (o instanceof Collection) {
                writeToStream(((Collection) o).toArray(), out, charestName);
            } else if (o instanceof Object[]) {
                writeToStream((Object[]) o, out, charestName);
            } else {
                throw new TDHSEncodeException("unkown type!");
            }

        }
    }


    private static void writeInt32ToStream(int v, ByteArrayOutputStream out) {
        for (int i = 3; i >= 0; i--) {
            out.write((byte) ((v >>> (8 * i)) & 0XFF));
        }
    }

    private static void writeInt8ToStream(int v, ByteArrayOutputStream out) {
        out.write((byte) (v & 0XFF));
    }
}
