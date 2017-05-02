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

import javax.resource.cci.ResourceAdapterMetaData;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * LMDbRAMetaData
 *
 * @version $Revision: $
 */
public class LMDbRAMetaData implements ResourceAdapterMetaData {

    private static final Properties PROPERTIES = new Properties();
    private static final String PROPERTIES_FILE = "net.betzel.lmdb.jca.properties";
    private static final String ADAPTER_NAME = "lmdb.ra.name";
    private static final String ADAPTER_VENDOR = "lmdb.ra.vendor";
    private static final String ADAPTER_VERSION = "lmdb.ra.version";
    private static final String ADAPTER_DESCRIPTION = "lmdb.ra.description";
    private static final String ADAPTER_JCA_VERSION = "lmdb.jca.version";

    static {
        try (InputStream inputStream = LMDbRAMetaData.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            PROPERTIES.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Default constructor
     */
    public LMDbRAMetaData() {

    }

    /**
     * Gets the version of the resource adapter.
     *
     * @return String representing version of the resource adapter
     */
    @Override
    public String getAdapterVersion() {
        return PROPERTIES.getProperty(ADAPTER_VERSION);
    }

    /**
     * Gets the name of the vendor that has provided the resource adapter.
     *
     * @return String representing name of the vendor
     */
    @Override
    public String getAdapterVendorName() {
        return PROPERTIES.getProperty(ADAPTER_VENDOR);
    }

    /**
     * Gets a tool displayable name of the resource adapter.
     *
     * @return String representing the name of the resource adapter
     */
    @Override
    public String getAdapterName() {
        return PROPERTIES.getProperty(ADAPTER_NAME);
    }

    /**
     * Gets a tool displayable short desription of the resource adapter.
     *
     * @return String describing the resource adapter
     */
    @Override
    public String getAdapterShortDescription() {
        return PROPERTIES.getProperty(ADAPTER_DESCRIPTION);
    }

    /**
     * Returns a string representation of the version
     *
     * @return String representing the supported version of the connector architecture
     */
    @Override
    public String getSpecVersion() {
        return PROPERTIES.getProperty(ADAPTER_JCA_VERSION);
    }

    /**
     * Returns an array of fully-qualified names of InteractionSpec
     *
     * @return Array of fully-qualified class names of InteractionSpec classes
     */
    @Override
    public String[] getInteractionSpecsSupported() {
        return null; //TODO
    }

    /**
     * Returns true if the implementation class for the Interaction
     *
     * @return boolean Depending on method support
     */
    @Override
    public boolean supportsExecuteWithInputAndOutputRecord() {
        return false; //TODO
    }

    /**
     * Returns true if the implementation class for the Interaction
     *
     * @return boolean Depending on method support
     */
    @Override
    public boolean supportsExecuteWithInputRecordOnly() {
        return false; //TODO
    }

    /**
     * Returns true if the resource adapter implements the LocalTransaction
     *
     * @return true If resource adapter supports resource manager local transaction demarcation
     */
    @Override
    public boolean supportsLocalTransactionDemarcation() {
        return false; //TODO
    }


}
