package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolTupleConversionException;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TarantoolCallResultMapperTest {

    private static final MessagePackMapper defaultMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    TarantoolTupleResultMapperFactory tarantoolTupleResultMapperFactory =
        TarantoolTupleResultMapperFactoryImpl.getInstance();
    private final
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
        defaultResultMapper = tarantoolTupleResultMapperFactory
        .withSingleValueArrayToTarantoolTupleResultMapper(defaultMapper, null);

    private static List<Object> nestedList1;
    private static TarantoolTuple tupleOne;
    private static List<Object> nestedList2;
    private static TarantoolTuple tupleTwo;

    @BeforeAll
    public static void setUp() {
        nestedList1 = Arrays.asList("nested", "array", 1);
        tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        nestedList2 = Arrays.asList("nested", "array", 2);
        tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
    }

    @Test
    void testSingleValueCallResultMapper() {
        //[nil, message]
        ArrayValue errorResult = ValueFactory.newArray(ValueFactory.newNil(), ValueFactory.newString("ERROR"));
        assertThrows(TarantoolInternalException.class, () -> defaultResultMapper.fromValue(errorResult));

        //[nil, {str=message, stack=stacktrace}]
        MapValue error = ValueFactory.newMap(
            ValueFactory.newString("str"),
            ValueFactory.newString("ERROR"),
            ValueFactory.newString("stack"),
            ValueFactory.newString("stacktrace")
        );
        ArrayValue errorResult1 = ValueFactory.newArray(ValueFactory.newNil(), error);
        assertThrows(TarantoolInternalException.class, () -> defaultResultMapper.fromValue(errorResult1));

        //[[[], ...]]
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
            tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));
        ArrayValue callResult = ValueFactory.newArray(testTuples);
        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result = defaultResultMapper.fromValue(callResult);
        TarantoolResult<TarantoolTuple> tuples = result.value();

        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));

        //[[[], ...], [message]]
        ArrayValue errorResult2 =
            ValueFactory.newArray(testTuples, ValueFactory.newArray(ValueFactory.newString("ERROR")));
        assertThrows(TarantoolInternalException.class, () -> defaultResultMapper.fromValue(errorResult2));

        //[[[], ...], [{str=message, stack=stacktrace}, {str=message, stack=stacktrace}]]
        MapValue error1 = ValueFactory.newMap(error.asMapValue().map());
        ArrayValue errors = ValueFactory.newArray(error, error1);
        ArrayValue errorResult3 = ValueFactory.newArray(testTuples, errors);
        assertThrows(TarantoolInternalException.class, () -> defaultResultMapper.fromValue(errorResult3));
    }

    @Test
    void testDefaultTarantoolTupleResponse_singleResultShouldThrowException_cornerCase() {
        ArrayValue testTuples = ValueFactory.newArray(
            tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        // The second value is a MsgPack array, and it is parsed an error. However, the result format is
        // actually wrong, so the exact error contents does not matter in this case.
        assertThrows(TarantoolInternalException.class, () -> defaultResultMapper.fromValue(testTuples), "def");
    }

    @Test
    void testDefaultTarantoolTupleResponse_singleResultShouldThrowException() {
        ArrayValue testTuples = ValueFactory.newArray(tupleOne.toMessagePackValue(defaultMapper));

        // Another corner case, tuple result mapper should not be used for this result format
        assertThrows(TarantoolTupleConversionException.class, () -> defaultResultMapper.fromValue(testTuples));
    }

    @Test
    void testDefaultTarantoolTupleResponse_singleResultShouldThrowException_tooManyValues() {
        ArrayValue testTuples = ValueFactory.newArray(tupleOne.toMessagePackValue(defaultMapper),
            tupleOne.toMessagePackValue(defaultMapper), tupleOne.toMessagePackValue(defaultMapper));

        assertThrows(TarantoolFunctionCallException.class, () -> defaultResultMapper.fromValue(testTuples));
    }

    @Test
    void testMultiValueCallResultMapper() {
        CallResultMapper<TarantoolResult<TarantoolTuple>,
            MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>> mapper =
            tarantoolTupleResultMapperFactory
                .withMultiValueArrayToTarantoolTupleResultMapper(defaultMapper, null);

        //[[], ...]
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
            tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>> result = mapper.fromValue(testTuples);
        TarantoolResult<TarantoolTuple> tuples = result.value();

        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));
    }

    @Test
    void testResponseWithError() {
        ArrayValue resultWithError = ValueFactory.newArray(
            ValueFactory.newNil(), ValueFactory.newString("Error message from server")
        );

        TarantoolInternalException e = assertThrows(TarantoolInternalException.class,
            () -> defaultResultMapper.fromValue(resultWithError));
        assertEquals("Error message from server", e.getMessage());
    }

    @Test
    void testNilResponse() {
        ArrayValue nilResult = ValueFactory.newArray(ValueFactory.newNil());

        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result = defaultResultMapper.fromValue(nilResult);

        assertNull(result.value());
    }

    @Test
    void testNotUnpackedTable() {
        ArrayValue testTuples = ValueFactory.newArray(
            tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        //[[[], [], ...]]
        ArrayValue resultNotUnpacked = ValueFactory.newArray(testTuples);

        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result =
            defaultResultMapper.fromValue(resultNotUnpacked);
        TarantoolResult<TarantoolTuple> tuples = result.value();
        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));
    }
}
