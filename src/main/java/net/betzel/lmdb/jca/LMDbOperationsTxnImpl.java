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
package net.betzel.lmdb.jca;

import org.lmdbjava.Dbi;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;

/**
 * Created by mbetzel on 20.04.2017.
 * Redirects database mutations to the transactional database
 */
public class LMDbOperationsTxnImpl extends LMDbOperationsImpl {

    private ByteBuffer xid;
    private Dbi<ByteBuffer> dbiTxn;

    public LMDbOperationsTxnImpl(Dbi<ByteBuffer> dbi, Dbi<ByteBuffer> dbiTxn) {
        super(dbi);
        this.dbiTxn = dbiTxn;
    }

    @Override
    public void close() {
        super.close();
        dbiTxn.close();
    }

    @Override
    public boolean delete(ByteBuffer key) {
        dbiTxn.put(xid, LMDbUtil.toByteBuffer(new LMDbOperation(LMDbOperationType.DELETE_KEY, key)));
        return true;
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key) {
        return dbiTxn.put(txn, xid, LMDbUtil.toByteBuffer(new LMDbOperation(LMDbOperationType.DELETE_KEY, key)));
    }

    @Override
    public boolean delete(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val) {
        return dbiTxn.put(txn, xid, LMDbUtil.toByteBuffer(new LMDbOperation(LMDbOperationType.DELETE_KEY_VALUE, key, val)));
    }

    @Override
    public void drop(Txn<ByteBuffer> txn) {
        super.drop(txn);
    }

    @Override
    public boolean put(ByteBuffer key, ByteBuffer val) {
        dbiTxn.put(xid, LMDbUtil.toByteBuffer(new LMDbOperation(LMDbOperationType.PUT, key, val)));
        return true;
    }

    @Override
    public boolean put(Txn<ByteBuffer> txn, ByteBuffer key, ByteBuffer val, PutFlags... flags) {
        return dbiTxn.put(txn, xid, LMDbUtil.toByteBuffer(new LMDbOperation(LMDbOperationType.PUT, key, val)), flags);
    }

    @Override
    public Stat stat(Txn<ByteBuffer> txn) {
        return dbiTxn.stat(txn);
    }

    public Dbi<ByteBuffer> getDbi() {
        return dbiTxn;
    }

    public void setXid(Xid xid) {
        this.xid = LMDbUtil.toByteBuffer(xid);
    }

}