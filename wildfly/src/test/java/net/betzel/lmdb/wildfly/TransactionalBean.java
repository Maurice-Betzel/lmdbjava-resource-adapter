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
public class TransactionalBean {

    private static final Logger LOGGER = Logger.getLogger(TransactionalBean.class);

    @Inject
    Constants constants;

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory")
    private LMDbConnectionFactory testConnectionFactory;

    public void storeValues() throws ResourceException {
        LOGGER.info("storeValues");
        try (LMDbConnection connection = testConnectionFactory.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_1, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_1));
            connection.put(constants.DATABASE_KEY_2, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
        }
    }

    public void storeMoreValues() throws ResourceException {
        LOGGER.info("storeMoreValues");
        try (LMDbConnection connection = testConnectionFactory.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
            connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
        }
    }

    public void cleanUpDatabase() throws ResourceException {
        LOGGER.info("cleanUpDatabase");
        try (LMDbConnection connection = testConnectionFactory.getConnection(constants.DATABASE_NAME)) {
            String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class);
            assertEquals(constants.DATABASE_VAL_1, value1);
            String value2 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class);
            assertEquals(constants.DATABASE_VAL_2, value2);
            String value3 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class);
            assertEquals(constants.DATABASE_VAL_3, value3);
            String value4 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class);
            assertEquals(constants.DATABASE_VAL_4, value4);
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class));
        }
    }


    @Transactional(Transactional.TxType.REQUIRED)
    public void startTransaction() throws ResourceException {
        LOGGER.info("Running startNewTransaction");
        storeValues();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void startAnotherTransaction() throws ResourceException {
        LOGGER.info("Running startAnotherTransaction");
        storeMoreValues();
    }

}