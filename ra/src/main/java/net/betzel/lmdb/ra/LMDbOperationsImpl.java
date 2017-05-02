/*
    Copyright 2017 Maurice Betzel

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package net.betzel.lmdb.ra;

import org.lmdbjava.*;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by mbetzel on 20.04.2017.
 */
public class LMDbOperationsImpl implements LMDbOperations<ByteBuffer> {

    private Dbi<ByteBuffer> dbi;

    public LMDbOperationsImpl(Dbi<ByteBuffer> dbi) {
        this.dbi = dbi;
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
    public boolean put(ByteBuffer key, ByteBuffer val) {
        dbi.put(key, val);
        return true;
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

    public Dbi<ByteBuffer> getDbi() {
        return dbi;
    }

}