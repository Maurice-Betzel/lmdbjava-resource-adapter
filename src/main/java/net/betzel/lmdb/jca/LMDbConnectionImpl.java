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

import java.util.logging.Logger;

/**
 * LMDbConnectionImpl
 *
 * @version $Revision: $
 */
public class LMDbConnectionImpl implements LMDbConnection {
    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbConnectionImpl.class.getName());

    /**
     * ManagedConnection
     */
    private LMDbManagedConnection mc;

    /**
     * ManagedConnectionFactory
     */
    private LMDbManagedConnectionFactory mcf;

    /**
     * Default constructor
     *
     * @param mc  LMDbManagedConnection
     * @param mcf LMDbManagedConnectionFactory
     */
    public LMDbConnectionImpl(LMDbManagedConnection mc, LMDbManagedConnectionFactory mcf) {
        this.mc = mc;
        this.mcf = mcf;
    }

    /**
     * Call me
     */
    public void callMe() {
        if (mc != null)
            mc.callMe();
    }

    /**
     * Close
     */
    public void close() {
        if (mc != null) {
            mc.closeHandle(this);
            mc = null;
        }

    }

    /**
     * Set ManagedConnection
     */
    void setManagedConnection(LMDbManagedConnection mc) {
        this.mc = mc;
    }

}
