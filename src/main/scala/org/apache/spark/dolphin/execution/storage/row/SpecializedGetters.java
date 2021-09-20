package org.apache.spark.dolphin.execution.storage.row;


import org.apache.spark.dolphin.types.DataType;

public interface SpecializedGetters {

    boolean isNullAt(int ordinal);

    boolean getBoolean(int ordinal);

    int getInt(int ordinal);

    long getLong(int ordinal);

    float getFloat(int ordinal);

    double getDouble(int ordinal);

    String getString(int ordinal);

    Object get(int ordinal, DataType dataType);
}