package net.betzel.lmdb.jca;

import org.lmdbjava.*;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by mbetzel on 20.04.2017.
 */
public class LMDbDatabaseTxn implements LMDbDbi<ByteBuffer> {

    private Dbi<ByteBuffer> dbi;
    private Dbi<ByteBuffer> dbiTxn;
    private LMDbManagedConnection managedConnection;

    public LMDbDatabaseTxn(Dbi<ByteBuffer> dbi, Dbi<ByteBuffer> dbiTxn, LMDbManagedConnection managedConnection) {
        this.dbi = dbi;
        this.dbiTxn = dbiTxn;
        this.managedConnection = managedConnection;
    }

    @Override
    public void close() {
        dbiTxn.close();
    }

    @Override
    public boolean delete(ByteBuffer key) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        dbiTxn.put(LMDbUtil.serialize(xid), LMDbUtil.serialize(new LMDbKeyValueAction(LMDbAction.DELETE, key, null)));
        return true;
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.serialize(xid), LMDbUtil.serialize(new LMDbKeyValueAction(LMDbAction.DELETE, key, null)));
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.serialize(xid), LMDbUtil.serialize(new LMDbKeyValueAction(LMDbAction.DELETE, key, val)));
    }

    @Override
    public void drop(Txn<ByteBuffer> txn) {
        dbi.drop(txn);
    }

    @Override
    public ByteBuffer get(Txn<ByteBuffer> txn, ByteBuffer key) {
        return dbi.get(txn, key);
    }

    @Override
    public String getName() {
        return String.valueOf(UTF_8.decode(ByteBuffer.wrap(dbi.getName())));
    }

    @Override
    public CursorIterator<ByteBuffer> iterate(Txn<ByteBuffer> txn) {
        return dbi.iterate(txn);
    }

    @Override
    public CursorIterator<ByteBuffer> iterate(Txn<ByteBuffer> txn, CursorIterator.IteratorType type) {
        return dbi.iterate(txn, type);
    }

    @Override
    public CursorIterator<ByteBuffer> iterate(Txn<ByteBuffer> txn, ByteBuffer key, CursorIterator.IteratorType type) {
        return dbi.iterate(txn, key, type);
    }

    @Override
    public Cursor<ByteBuffer> openCursor(Txn<ByteBuffer> txn) {
        return dbi.openCursor(txn);
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer val) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        dbiTxn.put(LMDbUtil.serialize(xid), LMDbUtil.serialize(new LMDbKeyValueAction(LMDbAction.PUT, key, val)));
    }

    @Override
    public boolean put(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val, PutFlags... flags) {
        Xid xid = managedConnection.getXAResource().getAssociatedTransaction();
        return dbiTxn.put(txn, LMDbUtil.serialize(xid), LMDbUtil.serialize(new LMDbKeyValueAction(LMDbAction.PUT, key, val)), flags);
    }

    @Override
    public ByteBuffer reserve(Txn<ByteBuffer> txn, ByteBuffer key, int size, PutFlags... op) {
        return dbiTxn.reserve(txn, key, size, op);
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