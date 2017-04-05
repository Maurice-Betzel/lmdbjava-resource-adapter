/*
    Copyright 2017 Maurice Betzel

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package net.betzel.lmdb.jca;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.logging.Logger;

import org.lmdbjava.*;

/**
 * LMDbConnectionImpl
 *
 * @version $Revision: $
 */
public class LMDbConnectionImpl implements LMDbConnection {
    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbConnectionImpl.class.getName());

    private Dbi<ByteBuffer> dbi;

    /**
     * ManagedConnection
     */
    private LMDbManagedConnection managedConnection;

    /**
     * ManagedConnectionFactory
     */
    private LMDbManagedConnectionFactory managedConnectionFactory;

    /**
     * Default constructor
     *
     * @param managedConnection        LMDbManagedConnection
     * @param managedConnectionFactory LMDbManagedConnectionFactory
     */
    public LMDbConnectionImpl(LMDbManagedConnection managedConnection, LMDbManagedConnectionFactory managedConnectionFactory) {
        this.managedConnection = managedConnection;
        this.managedConnectionFactory = managedConnectionFactory;
        this.dbi = managedConnection.getDbi();
    }

    @Override
    public String getDatabaseName() {
        return managedConnection.getDatabaseName();
    }

    @Override
    public boolean put(Integer key, Integer val) {
        return put(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean put(String key, Integer val) {
        checkKeySize(key);
        return put(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean put(Integer key, String val) {
        return put(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean put(String key, String val) {
        checkKeySize(key);
        return put(toByteBuffer(key), toByteBuffer(val));
    }

    private boolean put(ByteBuffer key, ByteBuffer val) {
        boolean isPut = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isPut = dbi.put(txn, key, val);
            txn.commit();
        }
        return isPut;
    }

    @Override
    public <T> T get(Integer key, Class<T> type) {
        return get(toByteBuffer(key), type);
    }

    @Override
    public <T> T get(String key, Class<T> type) {
        checkKeySize(key);
        return get(toByteBuffer(key), type);
    }

    private <T> T get(ByteBuffer key, Class<T> type) {
        Object value = null;
        try (Txn<ByteBuffer> txn = managedConnection.getReadTransaction()) {
            ByteBuffer foundBuffer = dbi.get(txn, key);
            if (foundBuffer != null) {
                if (type == String.class) {
                    return type.cast(UTF_8.decode(foundBuffer).toString());
                } else {
                    return type.cast(Integer.valueOf(foundBuffer.getInt()));
                }
            }
        }
        return null;
    }

    @Override
    public boolean delete(Integer key) {
        return delete(toByteBuffer(key));
    }

    @Override
    public boolean delete(String key) {
        checkKeySize(key);
        return delete(toByteBuffer(key));
    }

    private boolean delete(ByteBuffer key) {
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = dbi.delete(txn, key);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public boolean delete(Integer key, Integer val) {
        return delete(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean delete(Integer key, String val) {
        return delete(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean delete(String key, Integer val) {
        checkKeySize(key);
        return delete(toByteBuffer(key), toByteBuffer(val));
    }

    @Override
    public boolean delete(String key, String val) {
        checkKeySize(key);
        return delete(toByteBuffer(key), toByteBuffer(val));
    }

    private boolean delete(ByteBuffer key, ByteBuffer val) {
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = dbi.delete(txn, key, val);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public void clear() {
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            dbi.drop(txn);
            txn.commit();
        }
    }

    static ByteBuffer toByteBuffer(Integer integer) {
        ByteBuffer byteBuffer = allocateDirect(Integer.SIZE / Byte.SIZE);
        byteBuffer.putInt(integer.intValue()).flip();
        return byteBuffer;
    }

    static ByteBuffer toByteBuffer(String string) {
        byte[] stringBytes = string.getBytes(UTF_8);
        ByteBuffer stringBuffer = allocateDirect(stringBytes.length);
        stringBuffer.put(stringBytes).flip();
        return stringBuffer;
    }

    private void checkKeySize(String key) {
        if (key.getBytes(UTF_8).length > managedConnectionFactory.getDatabaseMaxKeySize()) {
            throw new IllegalArgumentException("Key exceed max key size: " + managedConnectionFactory.getDatabaseMaxKeySize());
        }
    }

    @Override
    public void close() {
        if (managedConnection != null) {
            managedConnection.closeHandle(this);
            managedConnection = null;
        }
    }

    /**
     * Set ManagedConnection
     */
    void setManagedConnection(LMDbManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

}
