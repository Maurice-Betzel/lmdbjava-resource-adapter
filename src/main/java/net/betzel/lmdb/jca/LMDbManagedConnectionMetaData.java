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

import javax.resource.ResourceException;

import javax.resource.spi.ManagedConnectionMetaData;

/**
 * LMDbManagedConnectionMetaData
 *
 * @version $Revision: $
 */
public class LMDbManagedConnectionMetaData implements ManagedConnectionMetaData {
    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbManagedConnectionMetaData.class.getName());

    /**
     * Default constructor
     */
    public LMDbManagedConnectionMetaData() {

    }

    /**
     * Returns Product name of the underlying EIS instance connected through the ManagedConnection.
     *
     * @return Product name of the EIS instance
     * @throws ResourceException Thrown if an error occurs
     */
    @Override
    public String getEISProductName() throws ResourceException {
        log.finest("getEISProductName()");
        return null; //TODO
    }

    /**
     * Returns Product version of the underlying EIS instance connected through the ManagedConnection.
     *
     * @return Product version of the EIS instance
     * @throws ResourceException Thrown if an error occurs
     */
    @Override
    public String getEISProductVersion() throws ResourceException {
        log.finest("getEISProductVersion()");
        return null; //TODO
    }

    /**
     * Returns maximum limit on number of active concurrent connections
     *
     * @return Maximum limit for number of active concurrent connections
     * @throws ResourceException Thrown if an error occurs
     */
    @Override
    public int getMaxConnections() throws ResourceException {
        log.finest("getMaxConnections()");
        return 0; //TODO
    }

    /**
     * Returns name of the user associated with the ManagedConnection instance
     *
     * @return Name of the user
     * @throws ResourceException Thrown if an error occurs
     */
    @Override
    public String getUserName() throws ResourceException {
        log.finest("getUserName()");
        return null; //TODO
    }


}
