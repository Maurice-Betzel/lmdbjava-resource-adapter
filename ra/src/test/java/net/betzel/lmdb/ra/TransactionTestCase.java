/*
    Copyright 2017 Maurice Betzel

    Licensed under the Apache License, Version 2.0 (the "License");
    you may noat use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package net.betzel.lmdb.ra;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.jca.arquillian.embedded.Inject;
import org.jboss.jca.core.spi.mdr.MetadataRepository;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Resource;
import javax.resource.ResourceException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * ConnectorTestCase
 *
 * @version $Revision: $
 */
@RunWith(Arquillian.class)
public class TransactionTestCase {

    private static Logger log = Logger.getLogger(TransactionTestCase.class.getName());

    private static String deploymentName = "TransactionTestCase";

    String databaseName = "testdb1";
    String databaseKey1 = "testKey1";
    String databaseVal1 = "testVal1";
    String databaseKey2 = "testKey2";
    String databaseVal2 = "testVal2";
    String databaseKey3 = "testKey3";
    String databaseVal3 = "testVal3";
    String databaseKey4 = "testKey4";
    String databaseVal4 = "testVal4";

    /**
     * Define the deployment
     *
     * @return The deployment archive
     */
    @Deployment
    public static ResourceAdapterArchive createDeployment() {
        ResourceAdapterArchive resourceAdapterArchive = ShrinkWrap.create(ResourceAdapterArchive.class, deploymentName + ".rar");
        JavaArchive javaArchive = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
        javaArchive.addPackages(true, Package.getPackage("net.betzel.lmdb.ra"));
        resourceAdapterArchive.addAsLibrary(javaArchive);
        resourceAdapterArchive.addAsManifestResource("ironjacamar.xml");
        return resourceAdapterArchive;
    }

    @Inject(name = "MDR")
    private MetadataRepository mdr;

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory")
    private LMDbConnectionFactory testConnectionFactory;

    @Resource(mappedName = "java:/UserTransaction")
    private UserTransaction userTransaction;

    @Resource(mappedName = "java:/TransactionManager")
    private TransactionManager transactionManager;

    /**
     * Test local transaction
     *
     * @throws Throwable Thrown if case of an error
     */

    @Test
    public void testTransaction() throws Throwable {
        log.finest("testTransaction()");
        assertNotNull(testConnectionFactory);
        assertNotNull(userTransaction);
        assertEquals(userTransaction.getStatus(), Status.STATUS_NO_TRANSACTION);

        userTransaction.begin();
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.put(databaseKey1, LMDbUtil.toByteBuffer(databaseVal1));
            connectionXA.put(databaseKey2, LMDbUtil.toByteBuffer(databaseVal2));
        }
        userTransaction.commit();

        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName)) {
            String value1 = connection.get(LMDbUtil.toByteBuffer(databaseKey1), String.class);
            assertEquals(value1, databaseVal1);
            String value2 = connection.get(LMDbUtil.toByteBuffer(databaseKey2), String.class);
            assertEquals(value2, databaseVal2);
        }

        userTransaction.begin();
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.delete(LMDbUtil.toByteBuffer(databaseKey1));
            connectionXA.delete(LMDbUtil.toByteBuffer(databaseKey2), LMDbUtil.toByteBuffer(databaseVal2));
        }
        userTransaction.commit();

        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName)) {
            String value1 = connection.get(LMDbUtil.toByteBuffer(databaseKey1), String.class);
            assertNull(value1);
            String value2 = connection.get(LMDbUtil.toByteBuffer(databaseKey2), String.class);
            assertNull(value2);
        }
    }

    @Test
    public void testTransactionSuspend() throws Throwable {
        log.finest("testTransactionSuspend()");
        assertNotNull(testConnectionFactory);
        assertNotNull(transactionManager);
        assertNotNull(userTransaction);

        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        userTransaction.begin();

        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        startNewTransaction();

        Transaction transaction = transactionManager.suspend(); // Suspend transaction from thread

        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        userTransaction.begin();

        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        startAnotherTransaction();

        userTransaction.commit();

        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        transactionManager.resume(transaction);

        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        userTransaction.commit();

        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName)) {
            String value1 = connection.get(LMDbUtil.toByteBuffer(databaseKey1), String.class);
            assertEquals(databaseVal1, value1);
            String value2 = connection.get(LMDbUtil.toByteBuffer(databaseKey2), String.class);
            assertEquals(databaseVal2, value2);
            String value3 = connection.get(LMDbUtil.toByteBuffer(databaseKey3), String.class);
            assertEquals(databaseVal3, value3);
            String value4 = connection.get(LMDbUtil.toByteBuffer(databaseKey4), String.class);
            assertEquals(databaseVal4, value4);
            connection.delete(LMDbUtil.toByteBuffer(databaseKey1));
            connection.delete(LMDbUtil.toByteBuffer(databaseKey2), LMDbUtil.toByteBuffer(databaseVal2));
            connection.delete(LMDbUtil.toByteBuffer(databaseKey3));
            connection.delete(LMDbUtil.toByteBuffer(databaseKey4), LMDbUtil.toByteBuffer(databaseVal4));
            assertNull(connection.get(LMDbUtil.toByteBuffer(databaseKey1), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(databaseKey2), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(databaseKey3), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(databaseKey4), String.class));
        }
    }

    private void startNewTransaction() throws ResourceException {
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.put(databaseKey1, LMDbUtil.toByteBuffer(databaseVal1));
            connectionXA.put(databaseKey2, LMDbUtil.toByteBuffer(databaseVal2));
            assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey1), String.class));
            assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey2), String.class));
        }
    }

    private void startAnotherTransaction() throws ResourceException {
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.put(databaseKey3, LMDbUtil.toByteBuffer(databaseVal3));
            connectionXA.put(databaseKey4, LMDbUtil.toByteBuffer(databaseVal4));
            assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey3), String.class));
            assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey4), String.class));
        }
    }

}