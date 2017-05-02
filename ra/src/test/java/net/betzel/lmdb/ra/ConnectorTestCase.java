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

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Resource;
import javax.resource.ResourceException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static java.lang.System.getProperty;
import static net.betzel.lmdb.ra.LMDbResourceAdapter.DISABLE_EXTRACT_PROP;
import static net.betzel.lmdb.ra.LMDbResourceAdapter.LMDB_NATIVE_LIB_PROP;
import static org.junit.Assert.*;

/**
 * ConnectorTestCase
 *
 * @version $Revision: $
 */
@RunWith(Arquillian.class)
public class ConnectorTestCase {

    private static Logger log = Logger.getLogger(ConnectorTestCase.class.getName());

    private static String deploymentName = "ConnectorTestCase";

    String databaseName1 = "testdb1";
    String databaseName2 = "testdb2";
    String databaseName3 = "testdb3";

    String databaseKey = "testKey";
    String databaseVal = "testVal";

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
        resourceAdapterArchive.addAsManifestResource("META-INF/ironjacamar.xml", "ironjacamar.xml");
        return resourceAdapterArchive;
    }

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory")
    private LMDbConnectionFactory testConnectionFactory;

    /**
     * Test connection and database creation
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testConnectionAndCreateDatabases() throws Throwable {
        log.finest("testConnectionAndCreateDatabases()");
        assertNotNull(testConnectionFactory);
        LMDbConnection connection1 = testConnectionFactory.getConnection(databaseName1);
        assertNotNull(connection1);
        assertEquals(connection1.getDatabaseName(), databaseName1);
        LMDbConnection connection2 = testConnectionFactory.getConnection(databaseName2);
        assertNotNull(connection2);
        assertEquals(connection2.getDatabaseName(), databaseName2);
        LMDbConnection connection3 = testConnectionFactory.getConnection(databaseName3);
        assertNotNull(connection3);
        assertEquals(connection3.getDatabaseName(), databaseName3);
        LMDbConnection connection4 = testConnectionFactory.getConnection(databaseName3);
        assertNotNull(connection4);
        assertEquals(connection4.getDatabaseName(), databaseName3);
        assertEquals(testConnectionFactory.getDatabaseNames().size(), 3);
        connection1.close();
        connection2.close();
        connection3.close();
        connection4.close();
    }

    @Test
    public void testGet() throws Throwable {
        testPut();
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try(LMDbConnection connection3 = testConnectionFactory.getConnection(databaseName3)) {
                    log.finest(connection3.get(LMDbUtil.toByteBuffer(databaseKey), String.class));
                } catch (ResourceException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void testPut() throws Throwable {
        try(LMDbConnection connection3 = testConnectionFactory.getConnection(databaseName3)) {
            log.finest(Boolean.toString(connection3.put(databaseKey, LMDbUtil.toByteBuffer(databaseVal))));
        }
    }

    /**
     * Test creation of system properties
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testSystemproperties() throws Throwable {
        log.finest("testSystemproperties()");
        assertNotNull(testConnectionFactory);
        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName1)) {
            assertNotNull(connection);
            assertNotNull(getProperty(DISABLE_EXTRACT_PROP));
            assertTrue(!Boolean.valueOf(getProperty(DISABLE_EXTRACT_PROP)));
            assertNull(getProperty(LMDB_NATIVE_LIB_PROP));
        }
    }

    /**
     * Test put, get and delete values
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testPutGetDelete() throws Throwable {
        log.finest("testPutGetDelete()");
        assertNotNull(testConnectionFactory);
        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName1)) {
            assertNotNull(connection);
            boolean result = connection.put(databaseKey, LMDbUtil.toByteBuffer(databaseVal));
            assertTrue(result);
            String value = connection.get(LMDbUtil.toByteBuffer(databaseKey), String.class);
            assertNotNull(value);
            assertEquals(databaseVal, value);
            result = connection.delete(LMDbUtil.toByteBuffer(databaseKey));
            assertTrue(result);
            value = connection.get(LMDbUtil.toByteBuffer(databaseKey), String.class);
            assertNull(value);
            // delete with key/value
            result = connection.put(LMDbUtil.toByteBuffer(databaseKey), LMDbUtil.toByteBuffer(databaseVal));
            assertTrue(result);
            value = connection.get(LMDbUtil.toByteBuffer(databaseKey), String.class);
            assertNotNull(value);
            assertEquals(databaseVal, value);
            result = connection.delete(LMDbUtil.toByteBuffer(databaseKey), LMDbUtil.toByteBuffer(databaseVal));
            assertTrue(result);
            value = connection.get(LMDbUtil.toByteBuffer(databaseKey), String.class);
            assertNull(value);
        }
    }

    /**
     * Test clear batabase
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testClear() throws Throwable {
        log.finest("testClear()");
        assertNotNull(testConnectionFactory);
        try (LMDbConnection connection = testConnectionFactory.getConnection(databaseName1)) {
            assertNotNull(connection);
            connection.put(LMDbUtil.toByteBuffer(1), LMDbUtil.toByteBuffer(42));
            connection.put(LMDbUtil.toByteBuffer(2), LMDbUtil.toByteBuffer(42));
            connection.put(LMDbUtil.toByteBuffer(3), LMDbUtil.toByteBuffer(42));
            Integer result = connection.get(LMDbUtil.toByteBuffer(1), Integer.class);
            assertEquals(42, result.intValue());
            result = connection.get(LMDbUtil.toByteBuffer(2), Integer.class);
            assertEquals(42, result.intValue());
            result = connection.get(LMDbUtil.toByteBuffer(3), Integer.class);
            assertEquals(42, result.intValue());
            connection.clear();
            result = connection.get(LMDbUtil.toByteBuffer(1), Integer.class);
            assertNull(result);
            result = connection.get(LMDbUtil.toByteBuffer(2), Integer.class);
            assertNull(result);
            result = connection.get(LMDbUtil.toByteBuffer(3), Integer.class);
            assertNull(result);
        }
    }

}