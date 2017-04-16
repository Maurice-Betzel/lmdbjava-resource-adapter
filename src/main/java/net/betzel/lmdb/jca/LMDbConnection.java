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

import javax.resource.spi.ConnectionRequestInfo;
import java.nio.ByteBuffer;

/**
 * LMDbConnection
 * <p>
 * Equals a Dbi
 *
 * @version $Revision: $
 */
public interface LMDbConnection extends AutoCloseable {

    String getDatabaseName();

    ConnectionRequestInfo getConnectionRequestInfo();

    void setManagedConnection(LMDbManagedConnection managedConnection);

    boolean put(String key, ByteBuffer val);

    boolean put(ByteBuffer key, ByteBuffer val);

    <T> T get(ByteBuffer key, Class<T> type);

    boolean delete(ByteBuffer key);

    boolean delete(ByteBuffer key, ByteBuffer val);

    void clear(); //drop

    void checkKeySize(int size);

    void checkKeySize(String key);

    @Override
    void close();

}