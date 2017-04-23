package net.betzel.lmdb.jca;

import org.lmdbjava.*;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;

/**
 * Created by mbetzel on 20.04.2017.
 * Redirects database mutations to the transactional database
 */
public class LMDbDatabaseTxn extends LMDbDatabase {

    private Dbi<ByteBuffer> dbiTxn;
    private LMDbManagedConnection managedConnection;

    public LMDbDatabaseTxn(Dbi<ByteBuffer> dbi, Dbi<ByteBuffer> dbiTxn, LMDbManagedConnection managedConnection) {
        super(dbi, managedConnection);
        this.dbiTxn = dbiTxn;
        this.managedConnection = managedConnection;
    }

    @Override
    public void close() {
        super.close();
        dbiTxn.close();
    }

    @Override
    public boolean delete(ByteBuffer key) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        dbiTxn.put(LMDbUtil.toByteBuffer(xid), LMDbUtil.toByteBuffer(new LMDbKeyValueAction(LMDbAction.DELETE_KEY, key)));
        return true;
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.toByteBuffer(xid), LMDbUtil.toByteBuffer(new LMDbKeyValueAction(LMDbAction.DELETE_KEY, key)));
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.toByteBuffer(xid), LMDbUtil.toByteBuffer(new LMDbKeyValueAction(LMDbAction.DELETE_KEY_VALUE, key, val)));
    }

    @Override
    public void drop(Txn<ByteBuffer> txn) {
        super.drop(txn);
        dbiTxn.drop(txn);
    }

    @Override
    public boolean put(ByteBuffer key, ByteBuffer val) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        dbiTxn.put(LMDbUtil.toByteBuffer(xid), LMDbUtil.toByteBuffer(new LMDbKeyValueAction(LMDbAction.PUT, key, val)));
        return true;
    }

    @Override
    public boolean put(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val, PutFlags... flags) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.toByteBuffer(xid), LMDbUtil.toByteBuffer(new LMDbKeyValueAction(LMDbAction.PUT, key, val)), flags);
    }

    @Override
    public Stat stat(Txn<ByteBuffer> txn) {
        return dbiTxn.stat(txn);
    }

    @Override
    public Dbi<ByteBuffer> getDbi() {
        return dbiTxn;
    }

}