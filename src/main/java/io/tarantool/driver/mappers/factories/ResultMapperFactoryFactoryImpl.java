package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.DefaultMultiValueResultMapper;
import io.tarantool.driver.mappers.DefaultSingleValueResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactoryImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToMultiValueListConverter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Manages instantiation of the operation result factories
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ResultMapperFactoryFactoryImpl implements ResultMapperFactoryFactory {

    /**
     * Basic constructor.
     */
    public ResultMapperFactoryFactoryImpl() {
    }

    @Override
    public ArrayValueToTarantoolTupleResultMapperFactory arrayTupleResultMapperFactory() {
        return new ArrayValueToTarantoolTupleResultMapperFactory();
    }

    @Override
    public TarantoolTupleResultMapperFactory getTarantoolTupleResultMapperFactory() {
        return TarantoolTupleResultMapperFactoryImpl.getInstance();
    }

    @Override
    public RowsMetadataToTarantoolTupleResultMapperFactory rowsMetadataTupleResultMapperFactory() {
        return new RowsMetadataToTarantoolTupleResultMapperFactory();
    }

    @Override
    public SingleValueWithTarantoolTupleResultMapperFactory singleValueTupleResultMapperFactory() {
        return new SingleValueWithTarantoolTupleResultMapperFactory();
    }

    @Override
    public MultiValueWithTarantoolTupleResultMapperFactory multiValueTupleResultMapperFactory() {
        return new MultiValueWithTarantoolTupleResultMapperFactory();
    }

    @Override
    public <T> ArrayValueToTarantoolResultMapperFactory<T> rowsMetadataStructureResultMapperFactory() {
        return new ArrayValueToTarantoolResultMapperFactory<>();
    }

    @Override
    public <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory() {
        return new SingleValueResultMapperFactory<>();
    }

    @Override
    public <T> SingleValueWithTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory() {
        return new SingleValueWithTarantoolResultMapperFactory<>();
    }

    @Override
    public <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory() {
        return new MultiValueResultMapperFactory<>();
    }

    @Override
    public <T> MultiValueWithTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory() {
        return new MultiValueWithTarantoolResultMapperFactory<>();
    }

    public <T> CallResultMapper<T, SingleValueCallResult<T>>
    getSingleValueResultMapper(ValueConverter<Value, T> valueConverter) {
        return this.<T>singleValueResultMapperFactory().withSingleValueResultConverter(
            valueConverter, (Class<SingleValueCallResult<T>>) (Class<?>) SingleValueCallResult.class);
    }

    public <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getMultiValueResultMapper(Supplier<R> containerSupplier, ValueConverter<Value, T> valueConverter) {
        return this.<T, R>multiValueResultMapperFactory().withMultiValueResultConverter(
            new ArrayValueToMultiValueListConverter<>(valueConverter, containerSupplier),
            (Class<MultiValueCallResult<T, R>>) (Class<?>) MultiValueCallResult.class);
    }

    public <T> CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    getTarantoolResultMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return this.<T>singleValueTarantoolResultMapperFactory()
            .withSingleValueArrayTarantoolResultConverter(getConverter(mapper, ValueType.ARRAY, tupleClass));
    }

    public <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getDefaultMultiValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultMultiValueResultMapper<>(mapper, tupleClass);
    }

    public <T> CallResultMapper<T, SingleValueCallResult<T>>
    getDefaultSingleValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultSingleValueResultMapper<>(mapper, tupleClass);
    }

    private <V extends Value, T> ValueConverter<V, T> getConverter(
        MessagePackMapper mapper, ValueType valueType, Class<T> tupleClass) {
        Optional<? extends ValueConverter<V, T>> converter = mapper.getValueConverter(valueType, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException(
                "No converter for value type %s and type %s is present", valueType, tupleClass);
        }
        return converter.get();
    }
}
