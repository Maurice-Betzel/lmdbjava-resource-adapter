package net.betzel.lmdb.wildfly;

import net.betzel.lmdb.ra.LMDbConnection;
import net.betzel.lmdb.ra.LMDbConnectionFactory;
import net.betzel.lmdb.ra.LMDbUtil;
import org.jboss.logging.Logger;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.resource.ResourceException;
import javax.transaction.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Maurice on 26.05.2017.
 */
@ApplicationScoped
public class CDITransactionalInnerBean {

    private static final Logger LOGGER = Logger.getLogger(CDITransactionalInnerBean.class);

    @Inject
    Constants constants;

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory2")
    private LMDbConnectionFactory testConnectionFactory;

    @Transactional(Transactional.TxType.REQUIRED)
    public void storeMoreValues() throws ResourceException {
        LOGGER.info("storeMoreValues");
        try (LMDbConnection connection = testConnectionFactory.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
            connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
        }
    }

}