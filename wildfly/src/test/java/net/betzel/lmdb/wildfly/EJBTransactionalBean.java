package net.betzel.lmdb.wildfly;

import net.betzel.lmdb.ra.LMDbConnection;
import net.betzel.lmdb.ra.LMDbConnectionFactory;
import net.betzel.lmdb.ra.LMDbUtil;
import org.jboss.logging.Logger;

import javax.annotation.Resource;
import javax.ejb.*;
import javax.inject.Inject;
import javax.resource.ResourceException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Maurice on 27.05.2017.
 */
@TransactionManagement(TransactionManagementType.BEAN)
@Stateless
public class EJBTransactionalBean {

    private static final Logger LOGGER = Logger.getLogger(CDITransactionalBean.class);

    @Inject
    Constants constants;

    /**
     * Resource
     */
    @Resource(mappedName = "java:/eis/LMDbConnectionFactory1")
    private LMDbConnectionFactory testConnectionFactory1;

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void storeValues() throws ResourceException {
        LOGGER.info("EJB storeValues");
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_1, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_1));
            connection.put(constants.DATABASE_KEY_2, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
        }
    }

    @TransactionAttribute(TransactionAttributeType.NEVER)
    public void storeMoreValues() throws ResourceException {
        LOGGER.info("EJB storeMoreValues");
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
            connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
        }
    }

    public void cleanUpDatabase() throws ResourceException {
        LOGGER.info("EJB cleanUpDatabase");
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class);
            assertEquals(constants.DATABASE_VAL_1, value1);
            String value2 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class);
            assertEquals(constants.DATABASE_VAL_2, value2);
            String value3 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class);
            assertEquals(constants.DATABASE_VAL_3, value3);
            String value4 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class);
            assertEquals(constants.DATABASE_VAL_4, value4);
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3));
            connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class));
            assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class));
        }
    }

}