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
import javax.resource.ResourceException;
import javax.transaction.*;
import java.io.File;
import java.util.UUID;

/**
 * Created by Maurice on 26.05.2017.
 */
@RunWith(Arquillian.class)
public class DeclarativeTransactionTest {

    private static final Logger LOGGER = Logger.getLogger(DeclarativeTransactionTest.class);

    @Deployment
    public static EnterpriseArchive deploy() {
        JavaArchive testJar = ShrinkWrap.create(JavaArchive.class).addClasses(DeclarativeTransactionTest.class, CDITransactionalBean.class, Constants.class).addAsManifestResource("beans.xml");
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
    CDITransactionalBean cdiTransactionalBean;

    @Resource(mappedName = "java:/eis/LMDbConnectionFactory1")
    private LMDbConnectionFactory connectionFactory1;

    @Resource(mappedName = "java:/eis/LMDbConnectionFactory2")
    private LMDbConnectionFactory connectionFactory2;

    //TM does one-phase commit (1PC) optimization on the only RM involved in the transaction
    @Test
    @InSequence(0)
    public void onePhaseCommit() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
        LOGGER.info("Container managed CDI multiple transactions");
        cdiTransactionalBean.requiredTransactionOnePhaseCommit(connectionFactory1);
        cdiTransactionalBean.assertDatabase(connectionFactory1);
    }

    //TM does two-phase commit (2PC) optimization on the RMs involved in the transaction
    @Test
    @InSequence(1)
    public void twoPhaseCommit() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
        LOGGER.info("Container managed CDI multiple transactions");
        cdiTransactionalBean.requiredTransactionTwoPhaseCommit(connectionFactory1, connectionFactory2);
        cdiTransactionalBean.assertDatabase(connectionFactory1);
        cdiTransactionalBean.assertDatabase(connectionFactory2);
    }

//    @Test
//    @InSequence(1)
//    public void suspendTransaction() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
//        cdiTransactionalBean.startTransactionWithSuspend();
//        //cdiTransactionalBean.cleanUpDatabase();
//    }


}