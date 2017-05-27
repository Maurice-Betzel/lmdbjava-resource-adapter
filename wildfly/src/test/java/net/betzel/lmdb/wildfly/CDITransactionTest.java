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
public class CDITransactionTest {

    private static final Logger LOGGER = Logger.getLogger(CDITransactionTest.class);


    @Deployment
    public static EnterpriseArchive deploy() {
        JavaArchive testJar = ShrinkWrap.create(JavaArchive.class).addClasses(CDITransactionTest.class, TransactionalBean.class, Constants.class).addAsManifestResource("beans.xml");
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
    TransactionalBean transactionalBean;

    @Resource(mappedName = "java:/TransactionManager")
    TransactionManager transactionManager;

    @Test
    @InSequence(0)
    public void cdi_test() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, ResourceException {
        transactionalBean.startTransaction();
        transactionalBean.startAnotherTransaction();
        transactionalBean.cleanUpDatabase();
    }

}