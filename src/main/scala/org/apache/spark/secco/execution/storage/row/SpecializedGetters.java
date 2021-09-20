package org.apache.spark.secco.execution.storage.row;


import org.apache.spark.secco.types.DataType;

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