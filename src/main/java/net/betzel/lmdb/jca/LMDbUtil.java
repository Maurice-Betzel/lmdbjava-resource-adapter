package net.betzel.lmdb.jca;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by mbetzel on 06.04.2017.
 */
public class LMDbUtil {

    private LMDbUtil() {
    }

    public static ByteBuffer toByteBuffer(Short number) {
        ByteBuffer byteBuffer = allocateDirect(Short.SIZE / Byte.SIZE);
        byteBuffer.putShort(number.shortValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Long number) {
        ByteBuffer byteBuffer = allocateDirect(Long.SIZE / Byte.SIZE);
        byteBuffer.putLong(number.longValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Float number) {
        ByteBuffer byteBuffer = allocateDirect(Float.SIZE / Byte.SIZE);
        byteBuffer.putFloat(number.floatValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Double number) {
        ByteBuffer byteBuffer = allocateDirect(Double.SIZE / Byte.SIZE);
        byteBuffer.putDouble(number.doubleValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Integer number) {
        ByteBuffer byteBuffer = allocateDirect(Integer.SIZE / Byte.SIZE);
        byteBuffer.putInt(number.intValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(String string) {
        byte[] stringBytes = string.getBytes(UTF_8);
        ByteBuffer stringBuffer = allocateDirect(stringBytes.length);
        stringBuffer.put(stringBytes).flip();
        return stringBuffer;
    }

}