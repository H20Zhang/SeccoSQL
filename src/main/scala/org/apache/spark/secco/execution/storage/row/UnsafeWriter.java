package org.apache.spark.secco.execution.storage.row;

import org.apache.spark.secco.execution.storage.row.BufferHolder;
import sun.misc.Unsafe;

/**
 * Base class for writing Unsafe* structures.
 */
public abstract class UnsafeWriter {

    private Unsafe _UNSAFE = UnsafeInternalRow.UNSAFE();

    // Keep internal buffer holder
    protected final BufferHolder holder;

    // The offset of the global buffer where we start to write this structure.
    protected int startingOffset;

    protected UnsafeWriter(BufferHolder holder) {
        this.holder = holder;
    }

    /**
     * Accessor methods are delegated from BufferHolder class
     */
    public final BufferHolder getBufferHolder() {
        return holder;
    }

    public final byte[] getBuffer() {
        return holder.getBuffer();
    }

    public final void reset() {
        holder.reset();
    }

    public final int totalSize() {
        return holder.totalSize();
    }

    public final void grow(int neededSize) {
        holder.grow(neededSize);
    }

    public final int cursor() {
        return holder.getCursor();
    }

    public final void increaseCursor(int val) {
        holder.increaseCursor(val);
    }

    public final void setOffsetAndSizeFromPreviousCursor(int ordinal, int previousCursor) {
        setOffsetAndSize(ordinal, previousCursor, cursor() - previousCursor);
    }

    protected void setOffsetAndSize(int ordinal, int size) {
        setOffsetAndSize(ordinal, cursor(), size);
    }

    protected void setOffsetAndSize(int ordinal, int currentCursor, int size) {
        final long relativeOffset = currentCursor - startingOffset;
        final long offsetAndSize = (relativeOffset << 32) | (long)size;

        write(ordinal, offsetAndSize);
    }

    protected final void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 0x07) > 0) {
            _UNSAFE.putLong(getBuffer(), cursor() + ((numBytes >> 3) << 3), 0L);
        }
    }

    public abstract void setNull1Bytes(int ordinal);
    public abstract void setNull2Bytes(int ordinal);
    public abstract void setNull4Bytes(int ordinal);
    public abstract void setNull8Bytes(int ordinal);

    public abstract void write(int ordinal, boolean value);
    public abstract void write(int ordinal, byte value);
    public abstract void write(int ordinal, short value);
    public abstract void write(int ordinal, int value);
    public abstract void write(int ordinal, long value);
    public abstract void write(int ordinal, float value);
    public abstract void write(int ordinal, double value);
//    public abstract void write(int ordinal, Decimal input, int precision, int scale);

    public final void write(int ordinal, String input) {
        byte[] byteArray = input.getBytes();
        writeUnalignedBytes(ordinal, byteArray, UnsafeInternalRow.BYTE_ARRAY_OFFSET(), byteArray.length);
    }

    public final void write(int ordinal, byte[] input) {
        write(ordinal, input, 0, input.length);
    }

    public final void write(int ordinal, byte[] input, int offset, int numBytes) {
        writeUnalignedBytes(ordinal, input, UnsafeInternalRow.BYTE_ARRAY_OFFSET() + offset, numBytes);
    }

    private void writeUnalignedBytes(
            int ordinal,
            Object baseObject,
            long baseOffset,
            int numBytes) {
        final int roundedSize = UnsafeInternalRow.roundNumberOfBytesToNearestWord(numBytes);
        grow(roundedSize);
        zeroOutPaddingBytes(numBytes);
        _UNSAFE.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, numBytes);
        increaseCursor(roundedSize);
    }

//    public final void write(int ordinal, CalendarInterval input) {
//        // grow the global buffer before writing data.
//        grow(16);
//
//        if (input == null) {
//            BitSetMethods.set(getBuffer(), startingOffset, ordinal);
//        } else {
//            // Write the months, days and microseconds fields of interval to the variable length portion.
//            _UNSAFE.putInt(getBuffer(), cursor(), input.months);
//            _UNSAFE.putInt(getBuffer(), cursor() + 4, input.days);
//            _UNSAFE.putLong(getBuffer(), cursor() + 8, input.microseconds);
//        }
//        // we need to reserve the space so that we can update it later.
//        setOffsetAndSize(ordinal, 16);
//        // move the cursor forward.
//        increaseCursor(16);
//    }

//    public final void write(int ordinal, UnsafeRow row) {
//        writeAlignedBytes(ordinal, row.getBaseObject(), row.getBaseOffset(), row.getSizeInBytes());
//    }
//
//    public final void write(int ordinal, UnsafeMapData map) {
//        writeAlignedBytes(ordinal, map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
//    }
//
//    public final void write(UnsafeArrayData array) {
//        // Unsafe arrays both can be written as a regular array field or as part of a map. This makes
//        // updating the offset and size dependent on the code path, this is why we currently do not
//        // provide an method for writing unsafe arrays that also updates the size and offset.
//        int numBytes = array.getSizeInBytes();
//        grow(numBytes);
//        _UNSAFE.copyMemory(
//                array.getBaseObject(),
//                array.getBaseOffset(),
//                getBuffer(),
//                cursor(),
//                numBytes);
//        increaseCursor(numBytes);
//    }

//    private void writeAlignedBytes(
//            int ordinal,
//            Object baseObject,
//            long baseOffset,
//            int numBytes) {
//        grow(numBytes);
//        _UNSAFE.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
//        setOffsetAndSize(ordinal, numBytes);
//        increaseCursor(numBytes);
//    }

    protected final void writeBoolean(long offset, boolean value) {
        _UNSAFE.putBoolean(getBuffer(), offset, value);
    }

    protected final void writeByte(long offset, byte value) {
        _UNSAFE.putByte(getBuffer(), offset, value);
    }

    protected final void writeShort(long offset, short value) {
        _UNSAFE.putShort(getBuffer(), offset, value);
    }

    protected final void writeInt(long offset, int value) {
        _UNSAFE.putInt(getBuffer(), offset, value);
    }

    protected final void writeLong(long offset, long value) {
        _UNSAFE.putLong(getBuffer(), offset, value);
    }

    protected final void writeFloat(long offset, float value) {
        _UNSAFE.putFloat(getBuffer(), offset, value);
    }

    protected final void writeDouble(long offset, double value) {
        _UNSAFE.putDouble(getBuffer(), offset, value);
    }
}
