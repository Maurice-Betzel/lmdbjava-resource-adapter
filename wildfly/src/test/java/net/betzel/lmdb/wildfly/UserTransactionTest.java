package net.betzel.lmdb.wildfly;

import net.betzel.lmdb.ra.LMDbConnectionFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.logging.Logger;
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
import javax.transaction.*;
import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Arquillian.class)
public class UserTransactionTest {

    private static final Logger LOGGER = Logger.getLogger(UserTransactionTest.class);

    @Deployment
    public static EnterpriseArchive deploy() {
        JavaArchive testJar = ShrinkWrap.create(JavaArchive.class).addClasses(UserTransactionTest.class, TransactionalBean.class, Constants.class).addAsManifestResource("beans.xml");
        File[] files = Maven.resolver().loadPomFromFile("pom.xml").importRuntimeDependencies().resolve().withTransitivity().asFile();
        for(File file: files) {
            if(file.getName().endsWith(".rar")) {
                ResourceAdapterArchive resourceAdapterArchive = ShrinkWrap.createFromZipFile(ResourceAdapterArchive.class, file);
                resourceAdapterArchive.addAsManifestResource("ironjacamar.xml");
                return  ShrinkWrap.create(EnterpriseArchive.class, UUID.randomUUID().toString() + ".ear").
                        addAsModule(resourceAdapterArchive).
                        //addAsResource("jboss-deployment-structure.xml").
                        addAsLibrary(testJar);
            }
        }
        throw new IllegalArgumentException("Missing resource archive!");
    }

    @Inject
    Constants constants;

    @Inject
    UserTransaction userTransaction;

    @Inject
    TransactionalBean transactionalBean;

    @Resource(mappedName = "java:/TransactionManager")
    TransactionManager transactionManager;

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
        LOGGER.info("Running testTransactionSuspend");
        assertNotNull(userTransaction);
        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());
        userTransaction.begin();
        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
        transactionalBean.storeValues();
        Transaction transaction = transactionManager.suspend(); // Suspend the transaction currently associated with the calling thread
        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());
        userTransaction.begin();
        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
        transactionalBean.storeMoreValues();
        userTransaction.commit();
        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());
        transactionManager.resume(transaction);
        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
        userTransaction.commit();
        assertEquals(Status.STATUS_NO_TRANSACTION, transactionManager.getStatus());
        transactionalBean.cleanUpDatabase();
    }

}