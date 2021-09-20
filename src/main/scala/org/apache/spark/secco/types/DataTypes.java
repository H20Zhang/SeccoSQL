package org.apache.spark.secco.types;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 *
 */
public class DataTypes {
    /**
     * Gets the StringType object.
     */
    public static final org.apache.spark.secco.types.DataType StringType = StringType$.MODULE$;

    /**
     * Gets the BooleanType object.
     */
    public static final org.apache.spark.secco.types.DataType BooleanType = org.apache.spark.secco.types.BooleanType$.MODULE$;

    /**
     * Gets the DoubleType object.
     */
    public static final org.apache.spark.secco.types.DataType DoubleType = org.apache.spark.secco.types.DoubleType$.MODULE$;

    /**
     * Gets the FloatType object.
     */
    public static final org.apache.spark.secco.types.DataType FloatType = org.apache.spark.secco.types.FloatType$.MODULE$;

    /**
     * Gets the IntegerType object.
     */
    public static final org.apache.spark.secco.types.DataType IntegerType = org.apache.spark.secco.types.IntegerType$.MODULE$;

    /**
     * Gets the LongType object.
     */
    public static final org.apache.spark.secco.types.DataType LongType = org.apache.spark.secco.types.LongType$.MODULE$;


    /**
     * Creates a StructField by specifying the name ({@code name}), data type ({@code dataType}) and
     * whether values of this field can be null values ({@code nullable}).
     */
    public static StructField createStructField(
            String name,
            org.apache.spark.secco.types.DataType dataType,
            boolean nullable) {
        if (name == null) {
            throw new IllegalArgumentException("name should not be null.");
        }
        if (dataType == null) {
            throw new IllegalArgumentException("dataType should not be null.");
        }
        return new StructField(name, dataType, nullable);
    }

    /**
     * Creates a StructType with the given list of StructFields ({@code fields}).
     */
    public static StructType createStructType(List<StructField> fields) {
        return createStructType(fields.toArray(new StructField[fields.size()]));
    }

    /**
     * Creates a StructType with the given StructField array ({@code fields}).
     */
    public static StructType createStructType(StructField[] fields) {
        if (fields == null) {
            throw new IllegalArgumentException("fields should not be null.");
        }
        Set<String> distinctNames = new HashSet<>();
        for (StructField field : fields) {
            if (field == null) {
                throw new IllegalArgumentException(
                        "fields should not contain any null.");
            }

            distinctNames.add(field.name());
        }
        if (distinctNames.size() != fields.length) {
            throw new IllegalArgumentException("fields should have distinct names.");
        }

        return StructType$.MODULE$.apply(fields);
    }
}