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

import org.lmdbjava.*;

/**
 * Created by mbetzel on 20.04.2017.
 */
public interface LMDbOperations<T> {

    public static final String TXN = "Txn";
    
    void close();

    boolean delete(T key);

    boolean delete(Txn<T> txn, T key);

    boolean delete(Txn<T> txn, T key, T val);

    void drop(Txn<T> txn);

    T get(Txn<T> txn, T key);

    String getName();

    CursorIterator<T> iterate(Txn<T> txn);

    CursorIterator<T> iterate(Txn<T> txn, CursorIterator.IteratorType type);

    CursorIterator<T> iterate(Txn<T> txn, T key, CursorIterator.IteratorType type);

    Cursor<T> openCursor(Txn<T> txn);

    boolean put(T key, T val);

    boolean put(Txn<T> txn, T key, T val, PutFlags... flags);

    T reserve(Txn<T> txn, T key, int size, PutFlags... op);

    Stat stat(Txn<T> txn);

    //Dbi<T> getOperations();

}