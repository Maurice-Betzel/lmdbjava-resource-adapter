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

import static java.lang.System.getProperty;
import static java.util.Locale.ENGLISH;
import java.util.UUID;
import java.util.logging.Logger;
import javax.annotation.Resource;
import static net.betzel.lmdb.jca.LMDbResourceAdapter.DISABLE_EXTRACT_PROP;
import static net.betzel.lmdb.jca.LMDbResourceAdapter.LMDB_NATIVE_LIB_PROP;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * ConnectorTestCase
 *
 * @version $Revision: $
 */
@RunWith(Arquillian.class)
public class ConnectorTestCase {

    private static Logger log = Logger.getLogger(ConnectorTestCase.class.getName());

    private static String deploymentName = "ConnectorTestCase";

    private static boolean linux;
    private static boolean osx;
    private static boolean windows;

    static {
        final String os = getProperty("os.name");
        linux = os.toLowerCase(ENGLISH).startsWith("linux");
        osx = os.startsWith("Mac OS X");
        windows = os.startsWith("Windows");
    }

    /**
     * Define the deployment
     *
     * @return The deployment archive
     */
    @Deployment
    public static ResourceAdapterArchive createDeployment() {
        ResourceAdapterArchive resourceAdapterArchive = ShrinkWrap.create(ResourceAdapterArchive.class, deploymentName + ".rar");
        JavaArchive javaArchive = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
        javaArchive.addPackages(true, Package.getPackage("net.betzel.lmdb.jca"));
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
     * Test getConnection
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testGetConnection() throws Throwable {
        assertNotNull(testConnectionFactory);
        String databaseName = "testdb";
        LMDbConnection connection = testConnectionFactory.getConnection(databaseName);
        assertNotNull(connection);
        assertEquals(connection.getDatabaseName(), databaseName);
        connection.close();
    }

    /**
     * Test database creation
     *
     * @throws Throwable Thrown if case of an error
     */
//    @Test
//    public void testCreateDatabase() throws Throwable {
//        assertNotNull(testConnectionFactory);
//        String databaseName = "testdb";
//        LMDbConnection connection = testConnectionFactory.getConnection(databaseName);
//        assertNotNull(connection);
//        Path databaseFile = Paths.get(databaseRootPath, databaseFileName);
//        assertTrue(Files.exists(databaseFile));
//        if(windows || osx) {
//            assertEquals(databaseFileSize, databaseFile.toFile().length());
//        }
//        connection.close();
//    }

    /**
     * Test creation of system properties
     *
     * @throws Throwable Thrown if case of an error
     */
    @Test
    public void testSystemproperties() throws Throwable {
        assertNotNull(testConnectionFactory);
        String databaseName = "testdb";
        LMDbConnection connection = testConnectionFactory.getConnection(databaseName);
        assertNotNull(connection);
        assertNotNull(getProperty(DISABLE_EXTRACT_PROP));
        assertTrue(!Boolean.valueOf(getProperty(DISABLE_EXTRACT_PROP)));
        assertNull(getProperty(LMDB_NATIVE_LIB_PROP));
        connection.close();
    }

}
