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

/**
 * LMDbConnection
 * <p>
 * Equals a Dbi
 *
 * @version $Revision: $
 */
public interface LMDbConnection extends AutoCloseable {

    public String getDatabaseName();

    public boolean put(Integer key, Integer val);

    public boolean put(Integer key, String val);

    public boolean put(String key, Integer val);

    public boolean put(String key, String value);

    public <T> T get(Integer key, Class<T> type);

    public <T> T get(String key, Class<T> type);

    public boolean delete(Integer key);

    public boolean delete(String key);

    public boolean delete(Integer key, Integer value);

    public boolean delete(Integer key, String value);

    public boolean delete(String key, Integer value);

    public boolean delete(String key, String value);

    public void clear(); //drop

    @Override
    public void close();

}