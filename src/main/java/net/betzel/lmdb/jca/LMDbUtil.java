package net.betzel.lmdb.jca;

import org.lmdbjava.LmdbException;

import java.io.*;
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

    public static ByteBuffer toByteBuffer(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            byte[] bytes = bos.toByteArray();
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
            byteBuffer.put(bytes).flip();
            return byteBuffer;
        } catch (IOException e) {
            throw new LmdbException(e.getMessage(), e);
        }
    }

    public static ByteBuffer cloneByteBuffer(ByteBuffer original) {
        ByteBuffer clone = (original.isDirect()) ? ByteBuffer.allocateDirect(original.capacity()) : ByteBuffer.allocate(original.capacity());
        ByteBuffer readOnlyCopy = original.asReadOnlyBuffer();
        readOnlyCopy.flip();
        clone.put(readOnlyCopy);
        return clone;
    }

    public static Long toLong(ByteBuffer byteBuffer) {
        return Long.class.cast(byteBuffer.getLong());
    }

    public static Short toShort(ByteBuffer byteBuffer) {
        return Short.class.cast(byteBuffer.getShort());
    }

    public static Float toFloat(ByteBuffer byteBuffer) {
        return Float.class.cast(byteBuffer.getFloat());
    }

    public static Double toDouble(ByteBuffer byteBuffer) {
        return Double.class.cast(byteBuffer.getDouble());
    }

    public static Integer toInteger(ByteBuffer byteBuffer) {
        return Integer.class.cast(byteBuffer.getInt());
    }

    public static String toString(ByteBuffer byteBuffer) {
        return String.class.cast(String.valueOf(UTF_8.decode(byteBuffer)));
    }

    public static <T> T toObject(ByteBuffer byteBuffer, Class<T> type) {
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.get(buffer);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer); ObjectInput in = new ObjectInputStream(bis)) {
            Object object = in.readObject();
            return (T) object;
        } catch (IOException e) {
            throw new LmdbException(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new LmdbException(e.getMessage(), e);
        }
    }

}