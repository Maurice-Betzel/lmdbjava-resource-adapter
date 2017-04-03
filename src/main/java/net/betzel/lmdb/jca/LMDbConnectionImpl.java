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
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.logging.Logger;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Stat;

/**
 * LMDbConnectionImpl
 *
 * @version $Revision: $
 */
public class LMDbConnectionImpl<T> implements LMDbConnection<T> {
    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbConnectionImpl.class.getName());

    private Dbi<T> dbi;

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
    public LMDbConnectionImpl(LMDbManagedConnection managedConnection, LMDbManagedConnectionFactory managedConnectionFactory, Dbi<T> dbi) {
        this.managedConnection = managedConnection;
        this.managedConnectionFactory = managedConnectionFactory;
        this.dbi = dbi;
    }

    @Override
    public boolean delete(T key) {
        return false;
    }

    @Override
    public boolean delete(T key, T value) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public T get(T key) {
        return null;
    }

    public int getMaxKeySize() {
        return managedConnection.getEnvironment().getMaxKeySize();
    }

    @Override
    public String getDatabaseName() {
        return String.valueOf(UTF_8.decode(ByteBuffer.wrap(dbi.getName())));
    }

    @Override
    public CursorIterator<T> iterate() {
        return null;
    }

    @Override
    public CursorIterator<T> iterate(T key, CursorIterator.IteratorType type) {
        return null;
    }

    @Override
    public Cursor openCursor() {
        return null;
    }

    @Override
    public void put(T key, T val) {

    }

    @Override
    public boolean put(T key, T val, PutFlags... flags) {
        return false;
    }

    @Override
    public T reserve(T key, int size, PutFlags... op) {
        return null;
    }

    @Override
    public Stat stat() {
        return null;
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
