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
package net.betzel.lmdb.ra;

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
        return managedConnection.getLMDbOperations().getName();
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
        log.finest("put1()");
        byte[] stringBytes = key.getBytes(UTF_8);
        checkKeySize(stringBytes.length);
        ByteBuffer keyBuffer = allocateDirect(stringBytes.length);
        keyBuffer.put(stringBytes).flip();
        return put(keyBuffer, val);
    }

    @Override
    public boolean put(ByteBuffer key, ByteBuffer val) {
        log.finest("put2()");
        boolean isPut = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isPut = managedConnection.getLMDbOperations().put(txn, key, val);
            txn.commit();
        }
        return isPut;
    }

    @Override
    public <T> T get(ByteBuffer key, Class<T> type) {
        log.finest("get()");
        Object object = null;
        try (Txn<ByteBuffer> txn = managedConnection.getReadTransaction()) {
            ByteBuffer foundBuffer = managedConnection.getLMDbOperations().get(txn, key);
            if (foundBuffer != null) {
                if (type == String.class) {
                    object = LMDbUtil.toString(foundBuffer);
                } else if (type == Integer.class) {
                    object = LMDbUtil.toInteger(foundBuffer);
                } else if (type == Long.class) {
                    object = LMDbUtil.toLong(foundBuffer);
                } else if (type == Float.class) {
                    object = LMDbUtil.toFloat(foundBuffer);
                } else if (type == Short.class) {
                    object = LMDbUtil.toShort(foundBuffer);
                } else if (type == Double.class) {
                    object = LMDbUtil.toDouble(foundBuffer);
                } else {
                    object = LMDbUtil.toObject(foundBuffer, type);
                }
            }
        }
        return (T) object;
    }

    @Override
    public boolean delete(ByteBuffer key) {
        log.finest("delete1()");
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = managedConnection.getLMDbOperations().delete(txn, key);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public boolean delete(ByteBuffer key, ByteBuffer val) {
        log.finest("delete2()");
        boolean isDeleted = false;
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = managedConnection.getLMDbOperations().delete(txn, key, val);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public void clear() {
        log.finest("clear()");
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            managedConnection.getLMDbOperations().drop(txn);
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
    public void dump() {
        managedConnection.dump();
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
        return managedConnection.getLMDbOperations().getName().hashCode() * 31;
    }

}