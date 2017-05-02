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

import javax.resource.ResourceException;
import javax.resource.spi.*;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import java.util.logging.Logger;

import static java.lang.System.getProperty;
import static java.lang.System.setProperty;

/**
 * LMDbResourceAdapter
 *
 * @version $Revision: $
 */
@Connector(reauthenticationSupport = false, transactionSupport = TransactionSupport.TransactionSupportLevel.XATransaction)
public class LMDbResourceAdapter implements ResourceAdapter, java.io.Serializable {

    public static final String DISABLE_EXTRACT_PROP = "lmdbjava.disable.extract";

    public static final String LMDB_NATIVE_LIB_PROP = "lmdbjava.native.lib";

    /**
     * The serial version UID
     */
    private static final long serialVersionUID = 1L;

    /**
     * The logger
     */
    private static Logger log = Logger.getLogger(LMDbResourceAdapter.class.getName());

    /**
     * lmdbjavaDisableExtract
     */
    @ConfigProperty(defaultValue = "false")
    private Boolean lmdbjavaDisableExtract;

    /**
     * lmdbjavaNativeLibPath
     */
    @ConfigProperty(defaultValue = "")
    private String lmdbjavaNativeLibPath;

    /**
     * Default constructor
     */
    public LMDbResourceAdapter() {

    }

    /**
     * Set lmdbjavaDisableExtract
     *
     * @param lmdbjavaDisableExtract The value
     */
    public void setLmdbjavaDisableExtract(Boolean lmdbjavaDisableExtract) {
        this.lmdbjavaDisableExtract = lmdbjavaDisableExtract;
    }

    /**
     * Get lmdbjavaDisableExtract
     *
     * @return The value
     */
    public Boolean getLmdbjavaDisableExtract() {
        return lmdbjavaDisableExtract;
    }

    /**
     * Set lmdbjavaNativeLibPath
     *
     * @param lmdbjavaNativeLibPath The value
     */
    public void setLmdbjavaNativeLibPath(String lmdbjavaNativeLibPath) {
        this.lmdbjavaNativeLibPath = lmdbjavaNativeLibPath;
    }

    /**
     * Get lmdbjavaNativeLibPath
     *
     * @return The value
     */
    public String getLmdbjavaNativeLibPath() {
        return lmdbjavaNativeLibPath;
    }

    /**
     * This is called during the activation of a message endpoint.
     *
     * @param endpointFactory A message endpoint factory instance.
     * @param spec            An activation spec JavaBean instance.
     * @throws ResourceException generic exception
     */
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException {
        log.finest("endpointActivation()");

    }

    /**
     * This is called when a message endpoint is deactivated.
     *
     * @param endpointFactory A message endpoint factory instance.
     * @param spec            An activation spec JavaBean instance.
     */
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
        log.finest("endpointDeactivation()");

    }

    /**
     * This is called when a resource adapter instance is bootstrapped.
     *
     * @param ctx A bootstrap context containing references
     * @throws ResourceAdapterInternalException indicates bootstrap failure.
     */
    public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
        log.finest("start()");
        if(getProperty(DISABLE_EXTRACT_PROP) == null) {
            setProperty("lmdbjava.disable.extract", lmdbjavaDisableExtract.toString());
        }
        if(getProperty(LMDB_NATIVE_LIB_PROP) == null) {
            if(!lmdbjavaNativeLibPath.isEmpty()) {
                setProperty("lmdbjava.disable.extract", lmdbjavaNativeLibPath);
            }
        }
    }

    /**
     * This is called when a resource adapter instance is undeployed or
     * during application server shutdown.
     */
    public void stop() {
        log.finest("stop()");
        System.clearProperty(DISABLE_EXTRACT_PROP);
        System.clearProperty(LMDB_NATIVE_LIB_PROP);
    }

    /**
     * This method is called by the application server during crash recovery.
     *
     * @param specs An array of ActivationSpec JavaBeans
     * @return An array of XAResource objects
     * @throws ResourceException generic exception
     */
    public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
        log.finest("getXAResources()");
        return null;
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
        int result = 17;
        if (lmdbjavaDisableExtract != null)
            result += 31 * result + 7 * lmdbjavaDisableExtract.hashCode();
        else
            result += 31 * result + 7;
        if (lmdbjavaNativeLibPath != null)
            result += 31 * result + 7 * lmdbjavaNativeLibPath.hashCode();
        else
            result += 31 * result + 7;
        return result;
    }

    /**
     * Indicates whether some other object is equal to this one.
     *
     * @param other The reference object with which to compare.
     * @return true if this object is the same as the obj argument, false otherwise.
     */
    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof LMDbResourceAdapter))
            return false;
        boolean result = true;
        LMDbResourceAdapter obj = (LMDbResourceAdapter) other;
        if (result) {
            if (lmdbjavaDisableExtract == null)
                result = obj.getLmdbjavaDisableExtract() == null;
            else
                result = lmdbjavaDisableExtract.equals(obj.getLmdbjavaDisableExtract());
        }
        if (result) {
            if (lmdbjavaNativeLibPath == null)
                result = obj.getLmdbjavaNativeLibPath() == null;
            else
                result = lmdbjavaNativeLibPath.equals(obj.getLmdbjavaNativeLibPath());
        }
        return result;
    }

}
