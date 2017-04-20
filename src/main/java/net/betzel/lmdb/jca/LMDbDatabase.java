package net.betzel.lmdb.jca;

import org.lmdbjava.*;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by mbetzel on 20.04.2017.
 */
public class LMDbDatabase implements LMDbDbi<ByteBuffer> {

    private Dbi<ByteBuffer> dbi;
    private LMDbManagedConnection managedConnection;

    public LMDbDatabase(Dbi<ByteBuffer> dbi, LMDbManagedConnection managedConnection) {
        this.dbi = dbi;
        this.managedConnection = managedConnection;
    }

    @Override
    public void close() {
        dbi.close();
    }

    @Override
    public boolean delete(ByteBuffer key) {
        return dbi.delete(key);
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key) {
        return dbi.delete(txn, key);
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val) {
        return dbi.delete(txn, key, val);
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
        dbi.put(key, val);
    }

    @Override
    public boolean put(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val, PutFlags... flags) {
        return dbi.put(txn, key, val, flags);
    }

    @Override
    public ByteBuffer reserve(Txn<ByteBuffer> txn, ByteBuffer key, int size, PutFlags... op) {
        return dbi.reserve(txn, key, size, op);
    }

    @Override
    public Stat stat(Txn<ByteBuffer> txn) {
        return dbi.stat(txn);
    }

    @Override
    public Dbi<ByteBuffer> getDbi() {
        return dbi;
    }

}