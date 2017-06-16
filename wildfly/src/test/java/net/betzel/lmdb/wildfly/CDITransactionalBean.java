package net.betzel.lmdb.wildfly;

import net.betzel.lmdb.ra.LMDbConnection;
import net.betzel.lmdb.ra.LMDbConnectionFactory;
import net.betzel.lmdb.ra.LMDbUtil;
import org.jboss.logging.Logger;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.resource.ResourceException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Maurice on 26.05.2017.
 */
@ApplicationScoped
public class CDITransactionalBean {

    private static final Logger LOGGER = Logger.getLogger(CDITransactionalBean.class);

    @Inject
    Constants constants;

    @Resource(mappedName = "java:/TransactionManager")
    TransactionManager transactionManager;

    @Transactional(Transactional.TxType.REQUIRED)
    public void requiredTransactionOnePhaseCommit(LMDbConnectionFactory testConnectionFactory1) throws ResourceException, SystemException {
        LOGGER.info("requiredTransactionOnePhaseCommit");
        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            storeValue1(connection);
            storeValue2(connection);
        }
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            storeValue3(connection);
            storeValue4(connection);
        }
    }

    @Transactional(Transactional.TxType.REQUIRED)
    public void requiredTransactionTwoPhaseCommit(LMDbConnectionFactory testConnectionFactory1, LMDbConnectionFactory testConnectionFactory2) throws ResourceException, SystemException {
        LOGGER.info("requiredTransactionTwoPhaseCommit");
        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
            storeValue1(connection);
            storeValue2(connection);
            storeValue3(connection);
            storeValue4(connection);
        }
        try (LMDbConnection connection = testConnectionFactory2.getConnection(constants.DATABASE_NAME)) {
            connection.put(constants.DATABASE_KEY_1, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_1));
            connection.put(constants.DATABASE_KEY_2, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
            connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
            connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
        }
    }

    public void assertDatabase(LMDbConnectionFactory testConnectionFactory) throws ResourceException {
        try (LMDbConnection connection = testConnectionFactory.getConnection(constants.DATABASE_NAME)) {
            assertDatabaseValue1(connection);
            assertDatabaseValue2(connection);
            assertDatabaseValue3(connection);
            assertDatabaseValue4(connection);
        }
    }

    public void storeValue1(LMDbConnection connection) throws ResourceException {
        LOGGER.info("storeValue1");
        connection.put(constants.DATABASE_KEY_1, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_1));
    }

    public void storeValue2(LMDbConnection connection) throws ResourceException {
        LOGGER.info("storeValue2");
        connection.put(constants.DATABASE_KEY_2, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
    }

    public void storeValue3(LMDbConnection connection) throws ResourceException {
        LOGGER.info("storeValue3");
        connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
    }

    public void storeValue4(LMDbConnection connection) throws ResourceException {
        LOGGER.info("storeValue4");
        connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
    }

    public void assertDatabaseValue1(LMDbConnection connection) throws ResourceException {
        LOGGER.info("assertDatabaseValue1");
        String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class);
        assertEquals(constants.DATABASE_VAL_1, value1);
        connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1));
        assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_1), String.class));
    }

    public void assertDatabaseValue2(LMDbConnection connection) throws ResourceException {
        LOGGER.info("assertDatabaseValue2");
        String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class);
        assertEquals(constants.DATABASE_VAL_2, value1);
        connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2));
        assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_2), String.class));
    }

    public void assertDatabaseValue3(LMDbConnection connection) throws ResourceException {
        LOGGER.info("assertDatabaseValue3");
        String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class);
        assertEquals(constants.DATABASE_VAL_3, value1);
        connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3));
        assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_3), String.class));
    }

    public void assertDatabaseValue4(LMDbConnection connection) throws ResourceException {
        LOGGER.info("assertDatabaseValue4");
        String value1 = connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class);
        assertEquals(constants.DATABASE_VAL_4, value1);
        connection.delete(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4));
        assertNull(connection.get(LMDbUtil.toByteBuffer(constants.DATABASE_KEY_4), String.class));
    }



//       public void requiredLocalTransaction() throws ResourceException, SystemException {
//        LOGGER.info("requiredLocalTransaction");
//        assertEquals(Status.STATUS_ACTIVE, transactionManager.getStatus());
//        try (LMDbConnection connection = testConnectionFactory1.getConnection(constants.DATABASE_NAME)) {
//            connection.put(constants.DATABASE_KEY_1, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_1));
//            connection.put(constants.DATABASE_KEY_2, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_2));
//            connection.put(constants.DATABASE_KEY_3, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_3));
//            connection.put(constants.DATABASE_KEY_4, LMDbUtil.toByteBuffer(constants.DATABASE_VAL_4));
//        }
//    }

}