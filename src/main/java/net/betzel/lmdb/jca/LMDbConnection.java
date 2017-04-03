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

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Stat;

/**
 * LMDbConnection
 * <p>
 * Equals a Dbi
 *
 * @version $Revision: $
 */
public interface LMDbConnection<T> extends AutoCloseable {

    //dbi

    public boolean delete(final T key);

    public boolean delete(final T key, final T value);

    public void clear(); //drop

    public T get(final T key);

    public int getMaxKeySize();

    public String getDatabaseName();

    public CursorIterator<T> iterate();

    public CursorIterator<T> iterate(final T key, final CursorIterator.IteratorType type);

    public Cursor<T> openCursor();

    public void put(final T key, final T val);

    public boolean put(final T key, final T val, final PutFlags... flags);

    public T reserve(final T key, final int size, final PutFlags... op);

    public Stat stat();

    @Override
    public void close();

}