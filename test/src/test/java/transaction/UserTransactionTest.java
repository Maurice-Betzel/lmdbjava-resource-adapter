package transaction;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ArchivePath;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.FileAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.*;

@RunWith(Arquillian.class)
public class UserTransactionTest {

    @Deployment
    public static Archive<?> deploy() {
        JavaArchive javaArchive = ShrinkWrap.create(JavaArchive.class).addAsManifestResource("beans.xml");
        ResourceAdapterArchive resourceAdapterArchive = ShrinkWrap.create(ResourceAdapterArchive.class);
        return  javaArchive;
    }

    @Inject
    UserTransaction ut;

    @Test
    public void should_work_with_cdi() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ut.begin();
        ut.commit();
    }

    @Test
    public void should_work_with_jndi() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, NamingException {
        Context context = new InitialContext();
        UserTransaction ut = (UserTransaction) context.lookup("java:comp/UserTransaction");
        ut.begin();
        ut.commit();
    }
}
