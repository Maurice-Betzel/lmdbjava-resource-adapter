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

import java.util.List;

/**
 * LMDbConnection
 * <p>
 * Equals a Dbi
 *
 * @version $Revision: $
 */
public interface LMDbConnection extends AutoCloseable {

    public String getDatabaseName();

    public List<String> getDatabaseNames();

    public void put(String key, String value);

    public String get(String key);

    public boolean delete(String key);

    public boolean delete(String key, String value);

    public void clear(); //drop

    @Override
    public void close();

}