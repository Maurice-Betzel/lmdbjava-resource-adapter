package net.betzel.lmdb.jca;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by mbetzel on 06.04.2017.
 */
public class LMDbUtil {

    public static final Short byteSizeLong = Long.SIZE / Byte.SIZE;
    public static final Short byteSizeShort = Short.SIZE / Byte.SIZE;
    public static final Short byteSizeFloat = Float.SIZE / Byte.SIZE;
    public static final Short byteSizeDouble = Double.SIZE / Byte.SIZE;
    public static final Short byteSizeInteger = Integer.SIZE / Byte.SIZE;

    private LMDbUtil() {
    }

    public static ByteBuffer toByteBuffer(Long number) {
        ByteBuffer byteBuffer = allocateDirect(byteSizeLong);
        byteBuffer.putLong(number.longValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Short number) {
        ByteBuffer byteBuffer = allocateDirect(byteSizeShort);
        byteBuffer.putShort(number.shortValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Float number) {
        ByteBuffer byteBuffer = allocateDirect(byteSizeFloat);
        byteBuffer.putFloat(number.floatValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Double number) {
        ByteBuffer byteBuffer = allocateDirect(byteSizeDouble);
        byteBuffer.putDouble(number.doubleValue()).flip();
        return byteBuffer;
    }

    public static ByteBuffer toByteBuffer(Integer number) {
        ByteBuffer byteBuffer = allocateDirect(byteSizeInteger);
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