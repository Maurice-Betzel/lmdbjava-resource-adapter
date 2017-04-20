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

import org.lmdbjava.Txn;

import javax.resource.spi.ConnectionRequestInfo;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

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
    }

    @Override
    public String getDatabaseName() {
        return managedConnection.getLMDbDbi().getName();
    }

    /**
     * Set ManagedConnection
     */
    @Override
    public void setManagedConnection(LMDbManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

    @Override
    public ConnectionRequestInfo getConnectionRequestInfo() {
        return managedConnection.getCxRequestInfo();
    }

    @Override
    public boolean put(String key, ByteBuffer val) {
        log.finest("put() with string key");
        byte[] stringBytes = key.getBytes(UTF_8);
        checkKeySize(stringBytes.length);
        ByteBuffer keyBuffer = allocateDirect(stringBytes.length);
        keyBuffer.put(stringBytes).flip();
        return put(keyBuffer, val);
    }

    @Override
    public boolean put(ByteBuffer key, ByteBuffer val) {
        log.finest("put()");
        boolean isPut = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isPut = managedConnection.getLMDbDbi().put(txn, key, val);
            txn.commit();
        }
        return isPut;
    }

    @Override
    public <T> T get(ByteBuffer key, Class<T> type) {
        log.finest("get()");
        Object value = null;
        try (Txn<ByteBuffer> txn = managedConnection.getReadTransaction()) {
            ByteBuffer foundBuffer = managedConnection.getLMDbDbi().get(txn, key);
            if (foundBuffer != null) {
                if (type == String.class) {
                    return type.cast(String.valueOf(UTF_8.decode(foundBuffer)));
                } else if (type == Integer.class) {
                    return type.cast(foundBuffer.getInt());
                } else if (type == Long.class) {
                    return type.cast(foundBuffer.getLong());
                } else if (type == Float.class) {
                    return type.cast(foundBuffer.getFloat());
                } else if (type == Short.class) {
                    return type.cast(foundBuffer.getShort());
                } else if (type == Double.class) {
                    return type.cast(foundBuffer.getDouble());
                } else {
                    throw new IllegalArgumentException("Type not supported: " + type.getCanonicalName());
                }
            }
        }
        return null;
    }

    @Override
    public boolean delete(ByteBuffer key) {
        log.finest("delete1()");
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = managedConnection.getLMDbDbi().delete(txn, key);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public boolean delete(ByteBuffer key, ByteBuffer val) {
        log.finest("delete2()");
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = managedConnection.getLMDbDbi().delete(txn, key, val);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public void clear() {
        log.finest("clear()");
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            managedConnection.getLMDbDbi().drop(txn);
            txn.commit();
        }
    }

    @Override
    public void checkKeySize(int size) {
        if (size > managedConnectionFactory.getDatabaseMaxKeySize()) {
            throw new IllegalArgumentException("Key exceed max key size: " + managedConnectionFactory.getDatabaseMaxKeySize());
        }
    }

    @Override
    public void checkKeySize(String key) {
        if (key.getBytes(UTF_8).length > managedConnectionFactory.getDatabaseMaxKeySize()) {
            throw new IllegalArgumentException("Key exceed max key size: " + managedConnectionFactory.getDatabaseMaxKeySize());
        }
    }

    @Override
    public void close() {
        log.finest("close()");
        if (managedConnection != null) {
            managedConnection.closeHandle(this);
            managedConnection = null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LMDbConnectionImpl that = (LMDbConnectionImpl) o;

        return this.getDatabaseName().equals(that.getDatabaseName());
    }

    @Override
    public int hashCode() {
        return managedConnection.getLMDbDbi().getName().hashCode() * 31;
    }

}