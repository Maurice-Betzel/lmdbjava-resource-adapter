package net.betzel.lmdb.jca;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;

/**
 * Created by mbetzel on 20.04.2017.
 */
public class LMDbKeyValueAction implements Serializable {

    private static final long serialVersionUID = 31L;

    private int action;
    private byte[] key;
    private byte[] val;

    public LMDbKeyValueAction(LMDbAction action, ByteBuffer key, ByteBuffer val) {
        this.action = action.getAction();
        this.key = new byte[key.remaining()];
        this.val = new byte[val.remaining()];
        key.get(this.key);
        val.get(this.val);
    }

    public LMDbAction getAction() {
        return LMDbAction.parseLMDbAction(action);
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

}