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

import org.lmdbjava.CursorIterator;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import javax.resource.ResourceException;
import javax.resource.spi.*;
import javax.security.auth.Subject;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;

/**
 * LMDbManagedConnection
 *
 * @version $Revision: $
 */
public class LMDbManagedConnection implements ManagedConnection {

    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbManagedConnection.class.getName());

    /**
     * The logwriter
     */
    private PrintWriter logwriter;

    /**
     * ManagedConnectionFactory
     */
    private LMDbManagedConnectionFactory managedConnectionFactory;

    /**
     * Listeners
     */
    private List<ConnectionEventListener> listeners;

    /**
     * Connections
     */
    private Set<LMDbConnection> connections;

    private LMDbOperationsImpl operations;

    private LMDbOperationsTxnImpl operationsTxn;

    /**
     * The lmdb environment
     */
    private Env environment;

    private LMDbConnectionRequestInfo connectionRequestInfo;

    private LMDbXAResource xaResource;

    private LocalTransaction txResource;

    /**
     * Default constructor
     *
     * @param managedConnectionFactory managedConnectionFactory
     */
    public LMDbManagedConnection(LMDbManagedConnectionFactory managedConnectionFactory, Env environment, ConnectionRequestInfo cxRequestInfo) {
        this.managedConnectionFactory = managedConnectionFactory;
        this.environment = environment;
        this.connectionRequestInfo = (LMDbConnectionRequestInfo) cxRequestInfo;
        this.logwriter = null;
        this.listeners = Collections.synchronizedList(new ArrayList<ConnectionEventListener>(1));
        this.connections = new HashSet();
        createLMDbOperations(connectionRequestInfo);
    }

    /**
     * Creates a new connection handle for every underlying physical Dbi connection
     * represented by the ManagedConnection instance.
     *
     * @param subject       Security context as JAAS subject
     * @param cxRequestInfo ConnectionRequestInfo instance
     * @return generic Object instance representing the connection handle.
     * @throws ResourceException generic exception if operation fails
     */
    public Object getConnection(Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException {
        log.finest("getConnection()");
        if (cxRequestInfo.equals(connectionRequestInfo)) {
            LMDbConnection connection = new LMDbConnectionImpl(this, managedConnectionFactory);
            connections.add(connection);
            return connection;
        } else {
            throw new IllegalArgumentException("Wrong database connection request!");
        }
    }

    /**
     * Used by the container to change the association of an
     * application-level connection handle with a ManagedConnection instance.
     *
     * @param connection Application-level connection handle
     * @throws ResourceException generic exception if operation fails
     */
    public void associateConnection(Object connection) throws ResourceException {
        log.finest("associateConnection()");
        if (connection == null) {
            throw new ResourceException("Null connection handle");
        }
        if (!(connection instanceof LMDbConnectionImpl)) {
            throw new ResourceException("Wrong connection handle");
        }
        LMDbConnection handle = (LMDbConnection) connection;
        if (connectionRequestInfo.equals(handle.getConnectionRequestInfo())) {
            handle.setManagedConnection(this);
            connections.add(handle);
        } else {
            throw new ResourceException("Wrong connection handle");
        }
    }

    /**
     * Application server calls this method to force any cleanup on the ManagedConnection instance.
     *
     * @throws ResourceException generic exception if operation fails
     */
    public void cleanup() throws ResourceException {
        log.finest("cleanup()");
        for (LMDbConnection connection : connections) {
            connection.setManagedConnection(null);
        }
        connections.clear();
    }

    /**
     * Destroys the physical connection to the underlying resource manager.
     *
     * @throws ResourceException generic exception if operation fails
     */
    public void destroy() throws ResourceException {
        log.finest("destroy()");
        if (operations != null) {
            operations.close();
            operations = null;
        }
        if (operationsTxn != null) {
            operationsTxn.close();
            operationsTxn = null;
        }
    }

    /**
     * Adds a connection event listener to the ManagedConnection instance.
     *
     * @param listener A new ConnectionEventListener to be registered
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        log.finest("addConnectionEventListener()");
        if (listener == null) {
            throw new IllegalArgumentException("Listener is null");
        }
        listeners.add(listener);
    }

    /**
     * Removes an already registered connection event listener from the ManagedConnection instance.
     *
     * @param listener already registered connection event listener to be removed
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        log.finest("removeConnectionEventListener()");
        if (listener == null) {
            throw new IllegalArgumentException("Listener is null");
        }
        listeners.remove(listener);
    }

    /**
     * Close handle
     *
     * @param handle The handle
     */
    void closeHandle(LMDbConnection handle) {
        log.finest("closeHandle()");
        connections.remove(handle);
        ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
        event.setConnectionHandle(handle);
        for (ConnectionEventListener cel : listeners) {
            cel.connectionClosed(event);
        }
    }

    /**
     * Gets the log writer for this ManagedConnection instance.
     *
     * @return Character output stream associated with this Managed-Connection instance
     * @throws ResourceException generic exception if operation fails
     */
    public PrintWriter getLogWriter() throws ResourceException {
        log.finest("getLogWriter()");
        return logwriter;
    }

    /**
     * Sets the log writer for this ManagedConnection instance.
     *
     * @param out Character Output stream to be associated
     * @throws ResourceException generic exception if operation fails
     */
    public void setLogWriter(PrintWriter out) throws ResourceException {
        log.finest("setLogWriter()");
        logwriter = out;
    }

    /**
     * Returns an <code>javax.resource.spi.LocalTransaction</code> instance.
     *
     * @return LocalTransaction instance
     * @throws ResourceException generic exception if operation fails
     */
    public LocalTransaction getLocalTransaction() throws ResourceException {
        log.finest("getLocalTransaction()");
        return txResource == null ? new LMDbTXResource(this) : txResource;
    }

    /**
     * Returns an <code>javax.transaction.xa.XAresource</code> instance.
     *
     * @return XAResource instance
     */
    public LMDbXAResource getXAResource() {
        log.finest("getXAResource()");
        if (xaResource == null) {
            this.xaResource = new LMDbXAResource(this);
        }
        return xaResource;
    }

    /**
     * Gets the metadata information for this connection's underlying EIS resource manager instance.
     *
     * @return ManagedConnectionMetaData instance
     * @throws ResourceException generic exception if operation fails
     */
    public ManagedConnectionMetaData getMetaData() throws ResourceException {
        log.finest("getMetaData()");
        return new LMDbManagedConnectionMetaData(managedConnectionFactory.getMaxReaders());
    }

    public ConnectionRequestInfo getCxRequestInfo() {
        log.finest("getCxRequestInfo()");
        return connectionRequestInfo;
    }

    public List<ConnectionEventListener> getListeners() {
        log.finest("getListeners()");
        return listeners;
    }

    void createLMDbOperations(LMDbConnectionRequestInfo connectionRequestInfo) {
        log.finest("createLMDbOperations() " + connectionRequestInfo.getDatabaseName());
        if (operations == null) {
            operations = new LMDbOperationsImpl(environment.openDbi(connectionRequestInfo.getDatabaseName(), DbiFlags.MDB_CREATE));
        }
    }

    void createLMDbOperationsTxn() {
        log.finest("createLMDbOperationsTxn()");
        if (operationsTxn == null) {
            operationsTxn = new LMDbOperationsTxnImpl(operations.getDbi(), environment.openDbi(connectionRequestInfo.getDatabaseName() + LMDbOperations.TXN, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT));
        }
    }

    public LMDbOperations<ByteBuffer> getLMDbOperations() {
        if (xaResource.hasAssociatedTransaction()) {
            log.finest("getLMDbOperations() with TX");
            operationsTxn.setXid(xaResource.getAssociatedTransaction());
            return operationsTxn;
        } else {
            log.finest("getLMDbOperations() without TX");
            return operations;
        }
    }

    LMDbOperationsImpl getOperations() {
        return operations;
    }

    LMDbOperationsTxnImpl getOperationsTxn() {
        return operationsTxn;
    }

    Txn getWriteTransaction() {
        log.finest("getWriteTransaction()");
        return environment.txnWrite();
    }

    Txn getReadTransaction() {
        log.finest("getReadTransaction()");
        return environment.txnRead();
    }

    void dump() {
        List<byte[]> dbiNames = environment.getDbiNames();
        log.warning("DATABASE NAMES");
        for (byte[] bytes : dbiNames) {
            log.warning(String.valueOf(UTF_8.decode(ByteBuffer.wrap(bytes))));
        }
        log.warning("DBI");
        try (Txn<ByteBuffer> txn = environment.txnRead()) {
            try (CursorIterator<ByteBuffer> it = operations.getDbi().iterate(txn, FORWARD)) {
                log.warning("DBI has next: " + it.hasNext());
                for (final CursorIterator.KeyVal<ByteBuffer> kv : it.iterable()) {
                    log.warning(LMDbUtil.toString(kv.key()));
                    log.warning(LMDbUtil.toString(kv.val()));
                }
            }
        }
        log.warning("DBI_TXN");
        try (Txn<ByteBuffer> txn = environment.txnRead()) {
            try (CursorIterator<ByteBuffer> it = operationsTxn.getDbi().iterate(txn, FORWARD)) {
                log.warning("DBI TXN has next: " + it.hasNext());
                for (final CursorIterator.KeyVal<ByteBuffer> kv : it.iterable()) {
                    log.warning(LMDbUtil.toString(kv.key()));
                    log.warning(LMDbUtil.toString(kv.val()));
                }
            }
        }
    }

}