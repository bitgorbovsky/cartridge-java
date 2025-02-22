package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolTupleConversionException;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Basic TarantoolResult implementation
 *
 * @param <T> target result tuple type
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class TarantoolResultImpl<T> implements TarantoolResult<T> {

    protected List<T> tuples;

    protected TarantoolResultImpl() {
    }

    public TarantoolResultImpl(ArrayValue value, ValueConverter<ArrayValue, T> valueConverter) {
        // [[[],...]]
        setItems(value, valueConverter);
    }

    protected void setItems(ArrayValue itemsArray, ValueConverter<ArrayValue, T> valueConverter) {
        this.tuples = itemsArray.list().stream()
            .map(v -> {
                try {
                    return valueConverter.fromValue(v.asArrayValue());
                } catch (MessageTypeCastException e) {
                    throw new TarantoolTupleConversionException(v, e);
                }
            })
            .collect(Collectors.toList());
    }

    @Override
    public int size() {
        return this.tuples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.tuples.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return this.tuples.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.tuples.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return this.tuples.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return this.tuples.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return this.tuples.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return new HashSet<>(this.tuples).containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return this.tuples.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        return this.tuples.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.tuples.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.tuples.retainAll(c);
    }

    @Override
    public void clear() {
        this.tuples.clear();
    }

    @Override
    public T get(int index) {
        return this.tuples.get(index);
    }

    @Override
    public T set(int index, T element) {
        return this.tuples.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        this.tuples.add(index, element);
    }

    @Override
    public T remove(int index) {
        return this.tuples.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.tuples.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.tuples.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return this.tuples.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return this.tuples.listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return this.tuples.subList(fromIndex, toIndex);
    }
}
