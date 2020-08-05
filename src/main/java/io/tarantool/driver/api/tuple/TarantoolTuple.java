package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.protocol.Packable;
import org.msgpack.value.Value;

import java.util.List;
import java.util.Optional;

/**
 * Basic Tarantool atom of data
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTuple extends Iterable<TarantoolField>, Packable {
    /**
     * Get a tuple field by its position
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @return field or empty optional if the field position is out of tuple length
     */
    Optional<TarantoolField> getField(int fieldPosition);

    /**
     * Get a tuple field by its name
     *
     * @param fieldName the field name in space
     * @return field or empty optional if the field not exist in space
     */
    Optional<TarantoolField> getField(String fieldName);

    /**
     * Get all tuple fields as list
     *
     * @return all type fields as list
     */
    List<TarantoolField> getFields();

    /**
     * Get a tuple field value by its position specifying the target value type
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param objectClass the target value type class
     * @param <O> the target value type
     * @return nullable value of a field wrapped in optional
     * @throws TarantoolValueConverterNotFoundException if the converter for the target type is not found
     */
    <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass) throws TarantoolValueConverterNotFoundException;

    /**
     * Returns the number of fields in this tuple
     *
     * @return the number of fields in this tuple
     */
    int size();

    /**
     * Set a tuple field by its position
     *
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param value new field value
     */
    <V extends Value> void setField(int fieldPosition, V value);

    /**
     * Set a tuple field by its position
     *
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param value new field value
     */
     void setField(int fieldPosition, Object value);

    /**
     * Set a tuple field by its name
     *
     * @param fieldName the field name
     * @param value new field value
     */
     void setField(String fieldName, Object value);
}