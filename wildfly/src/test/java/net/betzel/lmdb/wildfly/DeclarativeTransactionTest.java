package net.betzel.lmdb.wildfly;

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
        JavaArchive testJar = ShrinkWrap.create(JavaArchive.class).addClasses(DeclarativeTransactionTest.class, CDITransactionalBean.class, CDITransactionalInnerBean.class, Constants.class).addAsManifestResource("beans.xml");
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

    @Resource(mappedName = "java:/TransactionManager")
    TransactionManager transactionManager;

    @Test
    @InSequence(0)
    public void multipleTransactions() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
        LOGGER.info("Container managed CDI multiple transactions");
        cdiTransactionalBean.startTransaction();
        int status = transactionManager.getStatus();
        cdiTransactionalBean.startAnotherTransaction();
        cdiTransactionalBean.cleanUpDatabase();
    }

//    @Test
//    @InSequence(1)
//    public void suspendTransaction() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
//        cdiTransactionalBean.startTransactionWithSuspend();
//        //cdiTransactionalBean.cleanUpDatabase();
//    }


}