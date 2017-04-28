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

import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by mbetzel on 05.04.2017.
 */
public class LMDbXAResource implements XAResource {

    private static Logger log = Logger.getLogger(LMDbXAResource.class.getName());

    private LMDbManagedConnection managedConnection;

    private List<Xid> xids = Collections.synchronizedList(new ArrayList());

    private Xid associatedTransaction;

    private volatile boolean onePhase = false;

    private volatile int tmFlag = -1;


    public LMDbXAResource(LMDbManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        log.finest("XA commit()");
        if (onePhase) {
            // 1PC
            this.onePhase = true;
        }
        Dbi<ByteBuffer> dbi = managedConnection.getOperations().getDbi();
        Dbi<ByteBuffer> dbiTxn = managedConnection.getOperationsTxn().getDbi();
        // one txn for all following ops
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            ByteBuffer key = LMDbUtil.toByteBuffer(xid);
            try (CursorIterator<ByteBuffer> cursorIterator = dbiTxn.iterate(txn, key, CursorIterator.IteratorType.FORWARD)) {
                while (cursorIterator.hasNext()) {
                    CursorIterator.KeyVal<ByteBuffer> keyVal = cursorIterator.next();
                    LMDbKeyValueAction action = LMDbUtil.toObject(keyVal.val(), LMDbKeyValueAction.class);
                    switch (action.getAction()) {
                        case DELETE_KEY:
                            dbi.delete(txn, action.getKey());
                            break;
                        case DELETE_KEY_VALUE:
                            dbi.delete(txn, action.getKey(), action.getVal());
                            break;
                        case PUT:
                            dbi.put(txn, action.getKey(), action.getVal());
                            break;
                        case DROP:
                            dbi.drop(txn);
                    }
                }
            }
            dbiTxn.delete(txn, key);
            txn.commit();
            xids.remove(xid);
        } catch (Exception e) {
            throw new XAException("Exception on commit: " + e.getMessage());
        }
        tmFlag = -1;
        associatedTransaction = null;
    }

    /*
    Global transactions are disassociated from the resource via the
    XAResource.end method.
     */
    @Override
    public void end(Xid xid, int i) throws XAException {
        log.finest("XA end()");
        if (i == TMSUSPEND) {
            log.finest("XA TMSUSPEND");
            tmFlag = i;
            associatedTransaction = null;
        } else if (i == TMFAIL) {
            log.finest("XA TMFAIL");
        } else if (i == TMSUCCESS) {
            log.finest("XA TMSUCCESS");
        } else {
            throw new XAException("Unknown XA resource flag: " + i);
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        log.finest("XA forget()");
        Dbi<ByteBuffer> dbiTxn = managedConnection.getOperationsTxn().getDbi();
        try (Txn<ByteBuffer> txn = managedConnection.getWriteTransaction()) {
            ByteBuffer key = LMDbUtil.toByteBuffer(xid);
            dbiTxn.delete(txn, key);
            txn.commit();
            xids.remove(xid);
        } catch (Exception e) {
            throw new XAException("Error on forget: " + e.getMessage());
        }
        tmFlag = -1;
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        log.finest("XA getTransactionTimeout()");
        return 0;
    }

    /*
    If the target transaction already has another XAResource object participating in the
    transaction, the Transaction Manager invokes the XAResource.isSameRM method to
    determine if the specified XAResource represents the same resource manager instance.
    This information allows the TM to group the resource managers who are performing
    work on behalf of the transaction.
    */
    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        log.finest("XA isSameRM()");
        return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        log.finest("XA prepare()");
        // get txn
        return 0;
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        log.finest("XA recover()");
        if (onePhase) {
            return new Xid[0];
        } else {
            return xids.toArray(new Xid[xids.size()]);
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        log.finest("XA rollback()");
        tmFlag = -1;
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        log.finest("XA setTransactionTimeout()");
        return false;
    }

    /*
    Global transactions are associated with a transactional resource via the
    XAResource.start method, and disassociated from the resource via the
    XAResource.end method. The resource adapter is responsible for internally
    maintaining an association between the resource connection object and the XAResource
    object. At any given time, a connection is associated with a single transaction or it is
    not associated with any transaction at all.
     */
    @Override
    public void start(Xid xid, int i) throws XAException {
        log.finest("XA start()");
        if (hasAssociatedTransaction()) {
            throw new XAException("Nested transaction!");
        }
        if (i == TMNOFLAGS) {
            log.finest("XA TMNOFLAGS");
            tmFlag = i;
            associatedTransaction = xid;
            managedConnection.createLMDbOperationsTxn();
        } else if (i == TMJOIN) {
            log.finest("XA TMJOIN");
            tmFlag = i;
        } else if (i == TMRESUME) {
            log.finest("XA TMRESUME");
            associatedTransaction = xid;
            tmFlag = i;
        } else {
            i = -1;
            throw new XAException("Unknown XA resource flag: " + i);
        }
        xids.add(xid);
    }

    Xid getAssociatedTransaction() {
        log.finest("getAssociatedTransaction()");
        return associatedTransaction;
    }

    boolean hasAssociatedTransaction() {
        log.finest("hasAssociatedTransaction()");
        return associatedTransaction != null;
    }

}
