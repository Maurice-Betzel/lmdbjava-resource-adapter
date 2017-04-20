package net.betzel.lmdb.jca;

import org.lmdbjava.*;

import java.nio.ByteBuffer;

/**
 * Created by mbetzel on 20.04.2017.
 */
public interface LMDbDbi<T> {

    public static final String TXN = "Txn";
    
    void close();

    boolean delete(T key);

    boolean delete(Txn<T> txn, T key);

    boolean delete(Txn<T> txn, T key, T val);

    void drop(Txn<T> txn);

    T get(Txn<T> txn, T key);

    String getName();

    CursorIterator<T> iterate(Txn<T> txn);

    CursorIterator<T> iterate(Txn<T> txn, CursorIterator.IteratorType type);

    CursorIterator<T> iterate(Txn<T> txn, T key,
                              CursorIterator.IteratorType type);

    Cursor<T> openCursor(Txn<T> txn);

    void put(T key, T val);

    boolean put(Txn<T> txn, T key, T val,
                PutFlags... flags);

    T reserve(Txn<T> txn, T key, int size,
              PutFlags... op);

    Stat stat(Txn<T> txn);

    Dbi<ByteBuffer> getDbi();

}