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

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.LocalTransaction;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Created by mbetzel on 05.04.2017.
 */
public class LMDbTXResource implements LocalTransaction {

    private static Logger log = Logger.getLogger(LMDbTXResource.class.getName());

    private LMDbManagedConnection managedConnection;
    private Txn<ByteBuffer> readTransaction;
    private Txn<ByteBuffer> writeTransaction;

    public LMDbTXResource(LMDbManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

    @Override
    public void begin() throws ResourceException {
        log.finest("begin()");
        ConnectionEvent event = new ConnectionEvent(managedConnection, ConnectionEvent.LOCAL_TRANSACTION_STARTED);
        for (ConnectionEventListener cel : managedConnection.getListeners()) {
            cel.connectionClosed(event);
        }
    }

    @Override
    public void commit() throws ResourceException {
        log.finest("commit()");
        ConnectionEvent event = new ConnectionEvent(managedConnection, ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
        for (ConnectionEventListener cel : managedConnection.getListeners()) {
            cel.connectionClosed(event);
        }
    }

    @Override
    public void rollback() throws ResourceException {
        log.finest("rollback()");
        ConnectionEvent event = new ConnectionEvent(managedConnection, ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
        for (ConnectionEventListener cel : managedConnection.getListeners()) {
            cel.connectionClosed(event);
        }
    }

}