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

import java.io.Serializable;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;

/**
 * Created by mbetzel on 20.04.2017.
 */
public class LMDbOperation implements Serializable {

    private static final long serialVersionUID = 31L;

    private int action;
    private byte[] key;
    private byte[] val;

    public LMDbOperation(LMDbOperationType action, ByteBuffer key) {
        this.action = action.getAction();
        this.key = new byte[key.remaining()];
        key.get(this.key);
    }

    public LMDbOperation(LMDbOperationType action, ByteBuffer key, ByteBuffer val) {
        this.action = action.getAction();
        this.key = new byte[key.remaining()];
        this.val = new byte[val.remaining()];
        key.get(this.key);
        val.get(this.val);
    }

    public LMDbOperationType getAction() {
        return LMDbOperationType.parseLMDbAction(action);
    }

    public ByteBuffer getKey() {
        ByteBuffer byteBuffer = allocateDirect(key.length);
        byteBuffer.put(key).flip();
        return byteBuffer;
    }

    public ByteBuffer getVal() {
        ByteBuffer byteBuffer = allocateDirect(val.length);
        byteBuffer.put(val).flip();
        return byteBuffer;
    }

    public boolean hasVal() {
        return val != null;
    }

}