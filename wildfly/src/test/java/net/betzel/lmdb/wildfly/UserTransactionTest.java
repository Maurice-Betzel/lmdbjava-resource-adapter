package net.betzel.lmdb.wildfly;

import net.betzel.lmdb.ra.LMDbConnection;
import net.betzel.lmdb.ra.LMDbConnectionFactory;
import net.betzel.lmdb.ra.LMDbUtil;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.ResourceException;
import javax.transaction.*;
import java.io.File;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class UserTransactionTest {

    String databaseName = "testdb1";
    String databaseKey1 = "testKey1";
    String databaseVal1 = "testVal1";
    String databaseKey2 = "testKey2";
    String databaseVal2 = "testVal2";
    String databaseKey3 = "testKey3";
    String databaseVal3 = "testVal3";
    String databaseKey4 = "testKey4";
    String databaseVal4 = "testVal4";

    @Deployment
    public static EnterpriseArchive deploy() {
        JavaArchive testJavaArchive = ShrinkWrap.create(JavaArchive.class).addClasses(UserTransactionTest.class).addAsManifestResource("beans.xml");
        File[] files = Maven.resolver().loadPomFromFile("pom.xml").importRuntimeDependencies().resolve().withTransitivity().asFile();
        for(File file: files) {
            if(file.getName().endsWith(".rar")) {
                ResourceAdapterArchive resourceAdapterArchive = ShrinkWrap.createFromZipFile(ResourceAdapterArchive.class, file);
                resourceAdapterArchive.addAsManifestResource("ironjacamar.xml");
                return  ShrinkWrap.create(EnterpriseArchive.class, UUID.randomUUID().toString() + ".ear").addAsModule(resourceAdapterArchive).addAsLibrary(testJavaArchive);
            }
        }
        throw new IllegalArgumentException("Missing resource archive!");
    }

    @Inject
    UserTransaction userTransaction;

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory")
    private LMDbConnectionFactory testConnectionFactory;

    @Test
    @InSequence(0)
    public void should_work_with_cdi() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        userTransaction.begin();
        userTransaction.commit();
    }

    @Test
    @InSequence(1)
    public void should_work_with_jndi() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, NamingException {
        Context context = new InitialContext();
        UserTransaction userTransaction = (UserTransaction) context.lookup("java:comp/UserTransaction");
        userTransaction.begin();
        userTransaction.commit();
    }

    @Test
    @InSequence(2)
    public void testTransactionSuspend() throws Throwable {
        assertNotNull(userTransaction);

        //assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        userTransaction.begin();

        //assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        startNewTransaction();

        //Transaction transaction = transactionManager.suspend(); // XAResource End / Suspend not called this way

        //assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        //userTransaction.begin();

        //assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        startAnotherTransaction();

        //userTransaction.commit();

        //assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

        //transactionManager.resume(transaction);

        //assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());

        userTransaction.commit();

        //assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());

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

    @Transactional(Transactional.TxType.REQUIRED)
    private void startNewTransaction() throws ResourceException {
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.put(databaseKey1, LMDbUtil.toByteBuffer(databaseVal1));
            connectionXA.put(databaseKey2, LMDbUtil.toByteBuffer(databaseVal2));
            //assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey1), String.class));
            //assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey2), String.class));
        }
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    private void startAnotherTransaction() throws ResourceException {
        try (LMDbConnection connectionXA = testConnectionFactory.getConnection(databaseName)) {
            connectionXA.put(databaseKey3, LMDbUtil.toByteBuffer(databaseVal3));
            connectionXA.put(databaseKey4, LMDbUtil.toByteBuffer(databaseVal4));
            //assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey3), String.class));
            //assertNull(connectionXA.get(LMDbUtil.toByteBuffer(databaseKey4), String.class));
        }
    }

}
