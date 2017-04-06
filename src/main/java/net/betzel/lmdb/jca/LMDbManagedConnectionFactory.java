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

import org.lmdbjava.BufferProxy;
import org.lmdbjava.ByteBufferProxy;
import org.lmdbjava.DirectBufferProxy;
import org.lmdbjava.Env;

import javax.resource.ResourceException;
import javax.resource.spi.*;
import javax.security.auth.Subject;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

/**
 * LMDbManagedConnectionFactory
 *
 * @version $Revision: $
 */
@ConnectionDefinition(connectionFactory = LMDbConnectionFactory.class,
        connectionFactoryImpl = LMDbConnectionFactoryImpl.class,
        connection = LMDbConnection.class,
        connectionImpl = LMDbConnectionImpl.class)
public class LMDbManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation {

    /**
     * The serial version UID
     */
    private static final long serialVersionUID = 1L;

    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbManagedConnectionFactory.class.getName());

    /**
     * The resource adapter
     */
    private ResourceAdapter resourceAdapter;

    private Env environment;

    /**
     * The logwriter
     */
    private PrintWriter logwriter;

    @ConfigProperty
    private String filePath;

    @ConfigProperty(defaultValue = "1048576")
    private long fileSize;

    @ConfigProperty(defaultValue = "8")
    private int maxReaders;

    @ConfigProperty(defaultValue = "1")
    private int maxDatabases;

    /**
     * Default constructor
     */
    public LMDbManagedConnectionFactory() {

    }

    /**
     * Creates a Connection Factory instance.
     *
     * @param cxManager ConnectionManager to be associated with created EIS connection factory instance
     * @return EIS-specific Connection Factory instance or javax.resource.cci.ConnectionFactory instance
     * @throws ResourceException Generic exception
     */
    public Object createConnectionFactory(ConnectionManager cxManager) throws ResourceException {
        log.finest("createConnectionFactory() managed");
        if(environment == null || environment.isClosed()) {
            environment = createEnvironment();
        }
        return new LMDbConnectionFactoryImpl(this, cxManager);
    }

    /**
     * Creates a Connection Factory instance.
     *
     * @return EIS-specific Connection Factory instance or javax.resource.cci.ConnectionFactory instance
     * @throws ResourceException Generic exception
     */
    public Object createConnectionFactory() throws ResourceException {
        log.finest("createConnectionFactory() non managed");
//        if(environment == null || environment.isClosed()) {
//            environment = createEnvironment();
//        }
        throw new ResourceException("This resource adapter doesn't support non-managed environments");
    }

    /**
     * Creates a new physical connection to the underlying EIS resource manager.
     *
     * @param subject       Caller's security information
     * @param cxRequestInfo Additional resource adapter specific connection request information
     * @return ManagedConnection instance
     * @throws ResourceException generic exception
     */
    public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException {
        log.finest("createManagedConnection()");
        return new LMDbManagedConnection(this, environment);
    }

    /**
     * Returns a matched connection from the candidate set of connections.
     *
     * @param connectionSet Candidate connection set
     * @param subject       Caller's security information
     * @param cxRequestInfo Additional resource adapter specific connection request information
     * @return ManagedConnection if resource adapter finds an acceptable match otherwise null
     * @throws ResourceException generic exception
     */
    public ManagedConnection matchManagedConnections(Set connectionSet, Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException {
        log.finest("matchManagedConnections()");
        ManagedConnection result = null;
        Iterator it = connectionSet.iterator();
        while (result == null && it.hasNext()) {
            ManagedConnection managedConnection = (ManagedConnection) it.next();
            if (managedConnection instanceof LMDbManagedConnection && cxRequestInfo instanceof LMDbConnectionRequestInfo) {
                LMDbManagedConnection lmdbManagedConnection = (LMDbManagedConnection) managedConnection;
                LMDbConnectionRequestInfo lmdbConnectionRequestInfo = (LMDbConnectionRequestInfo) cxRequestInfo;
                if(lmdbManagedConnection.getDatabaseName().equals(lmdbConnectionRequestInfo.getDatabaseName())) {
                    result = lmdbManagedConnection;
                }
            }
        }
        return result;
    }

    /**
     * Get the log writer for this ManagedConnectionFactory instance.
     *
     * @return PrintWriter
     * @throws ResourceException generic exception
     */
    public PrintWriter getLogWriter() throws ResourceException {
        log.finest("getLogWriter()");
        return logwriter;
    }

    /**
     * Set the log writer for this ManagedConnectionFactory instance.
     *
     * @param out PrintWriter - an out stream for error logging and tracing
     * @throws ResourceException generic exception
     */
    public void setLogWriter(PrintWriter out) throws ResourceException {
        log.finest("setLogWriter()");
        logwriter = out;
    }

    /**
     * Get the resource adapter
     *
     * @return The handle
     */
    public ResourceAdapter getResourceAdapter() {
        log.finest("getResourceAdapter()");
        return resourceAdapter;
    }

    /**
     * Set the resource adapter
     *
     * @param resourceAdapter The handle
     */
    public void setResourceAdapter(ResourceAdapter resourceAdapter) {
        log.finest("setResourceAdapter()");
        this.resourceAdapter = resourceAdapter;
    }

    /**
     * Creates a new lmdb environment according to the configuration properties
     */
    private Env createEnvironment() throws ResourceException {
        log.finest("createEnvironment()");
        Path path = Paths.get(filePath);
        Path parentPath = path.getParent();
        if (Files.notExists(parentPath)) {
            try {
                Files.createDirectories(parentPath);
            } catch (IOException e) {
                throw new ResourceException(e);
            }
        } else {
            if (!Files.isDirectory(parentPath)) {
                throw new ResourceException(parentPath.toString() + " is not a directory!");
            }
        }
        return Env.create(ByteBufferProxy.PROXY_SAFE).setMaxDbs(maxDatabases).setMaxReaders(maxReaders).setMapSize(fileSize).open(path.toFile(), MDB_NOSUBDIR);
    }

    public int getDatabaseMaxKeySize() {
        return environment.getMaxKeySize();
    }

    public List<String> getDatabaseNames() {
        List<byte[]> dbiNames = environment.getDbiNames();
        List<String> databaseNames = new ArrayList<>(dbiNames.size());
        for(byte[] bytes : dbiNames) {
            databaseNames.add(String.valueOf(UTF_8.decode(ByteBuffer.wrap(bytes))));
        }
        return databaseNames;
    }

    String getFilePath() {
        return filePath;
    }

    void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    long getFileSize() {
        return fileSize;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    int getMaxReaders() {
        return maxReaders;
    }

    void setMaxReaders(int maxReaders) {
        this.maxReaders = maxReaders;
    }

    int getMaxDatabases() {
        return maxDatabases;
    }

    void setMaxDatabases(int maxDatabases) {
        this.maxDatabases = maxDatabases;
    }

    private BufferProxy parseBufferProxy(String bufferProxy) {
        LMDbBufferProxyEnum LMDbBufferProxyEnum = net.betzel.lmdb.jca.LMDbBufferProxyEnum.valueOf(bufferProxy);
        switch (LMDbBufferProxyEnum) {
            case PROXY_OPTIMAL:
                return ByteBufferProxy.PROXY_OPTIMAL;
            case PROXY_SAFE:
                return ByteBufferProxy.PROXY_SAFE;
            case PROXY_DB:
                return DirectBufferProxy.PROXY_DB;
            default:
                throw new IllegalArgumentException("Unknown buffer proxy: " + bufferProxy);
        }
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
        int result = 17;
        if (filePath != null)
            result += 31 * result + 7 * filePath.hashCode();
        else
            result += 31 * result + 7;
        result += 31 * result + 7 * fileSize;
        result += 31 * result + 7 * maxReaders;
        result += 31 * result + 7 * maxDatabases;
        return result;
    }

    /**
     * Indicates whether some other object is equal to this one.
     *
     * @param other The reference object with which to compare.
     * @return true if this object is the same as the obj argument, false otherwise.
     */
    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof LMDbManagedConnectionFactory))
            return false;
        LMDbManagedConnectionFactory obj = (LMDbManagedConnectionFactory) other;
        boolean result = true;
        if (result) {
            if (filePath == null)
                result = obj.getFilePath() == null;
            else
                result = filePath.equals(obj.getFilePath());
        }
        if (result) {
            result = fileSize == obj.getFileSize();
        }
        if (result) {
            result = maxReaders == obj.getMaxReaders();
        }
        if (result) {
            result = maxDatabases == obj.getMaxDatabases();
        }
        return result;
    }

}
