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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
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

    private volatile boolean onePhase = false;

    public LMDbXAResource(LMDbManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        log.finest("commit()");
        if (onePhase) {
            // 1PC
            this.onePhase = true;
        }
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
        log.finest("end()");
    }

    @Override
    public void forget(Xid xid) throws XAException {
        log.finest("forget()");

    }

    @Override
    public int getTransactionTimeout() throws XAException {
        log.finest("getTransactionTimeout()");
        return 0;
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        log.finest("isSameRM()");
        return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        log.finest("prepare()");
        // get txn
        return 0;
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        log.finest("recover()");
        if (onePhase) {
            return new Xid[0];
        } else {
            return xids.toArray(new Xid[xids.size()]);
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        log.finest("rollback()");

    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        log.finest("setTransactionTimeout()");
        return false;
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        log.finest("start()");
        if (i == TMNOFLAGS) {
            log.finest("TMNOFLAGS");
            // create / get tx database with xid UID AXGTRIDSIZE+MAXBQUALSIZE as key (write as combined byte[]?)
            // value consists of the key/value action(s) to be preformed on the original database within this transaction
            // lmdb supports one key / many values, which fits nicely here. Mabe no need for extra XA DB?
            // local txn fetched from managedConnection and set on the connectionImpl to perform tx database mods
            // on commit copy tx database to original database with one local txn.
        } else if (i == TMJOIN) {
            log.finest("TMJOIN");
        } else if (i == TMRESUME) {
            log.finest("TMRESUME");
        } else {
            log.finest("UNKNOWN");
        }

        //managedConnection.getConnection()

        xids.add(xid);
    }

}
