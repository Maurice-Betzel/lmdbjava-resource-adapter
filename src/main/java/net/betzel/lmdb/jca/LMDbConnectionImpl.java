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
    public boolean put(String key, String val) {
        boolean isPut = false;
        byte[] keyBytes = key.getBytes(UTF_8);
        byte[] valBytes = val.getBytes(UTF_8);
        checkKeySize(keyBytes.length);
        ByteBuffer keyBuffer = allocateDirect(keyBytes.length);
        ByteBuffer valBuffer = allocateDirect(valBytes.length);
        keyBuffer.put(keyBytes).flip();
        valBuffer.put(valBytes).flip();
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isPut = dbi.put(txn, keyBuffer, valBuffer);
            txn.commit();
        }
        return isPut;
    }

    @Override
    public String get(String key) {
        String value = null;
        byte[] keyBytes = key.getBytes(UTF_8);
        checkKeySize(keyBytes.length);
        ByteBuffer keyBuffer = allocateDirect(keyBytes.length);
        keyBuffer.put(keyBytes).flip();
        try (Txn<ByteBuffer> txn = managedConnection.getReadTransaction()) {
            ByteBuffer foundBuffer = dbi.get(txn, keyBuffer);
            if (foundBuffer != null) {
                value = UTF_8.decode(foundBuffer).toString();
            }
        }
        return value;
    }

    @Override
    public boolean delete(String key) {
        boolean isDeleted = false;
        byte[] keyBytes = key.getBytes(UTF_8);
        checkKeySize(keyBytes.length);
        ByteBuffer keyBuffer = allocateDirect(keyBytes.length);
        keyBuffer.put(keyBytes).flip();
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = dbi.delete(txn, keyBuffer);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public boolean delete(String key, String val) {
        boolean isDeleted = false;
        byte[] keyBytes = key.getBytes(UTF_8);
        byte[] valBytes = val.getBytes(UTF_8);
        checkKeySize(keyBytes.length);
        ByteBuffer keyBuffer = allocateDirect(keyBytes.length);
        ByteBuffer valBuffer = allocateDirect(valBytes.length);
        keyBuffer.put(keyBytes).flip();
        valBuffer.put(valBytes).flip();
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            isDeleted = dbi.delete(txn, keyBuffer, valBuffer);
            txn.commit();
        }
        return isDeleted;
    }

    @Override
    public void clear() {

    }

    private void checkKeySize(int keySize) {
        if (keySize > managedConnectionFactory.getDatabaseMaxKeySize()) {
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
