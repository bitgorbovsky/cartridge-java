package io.tarantool.examples;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Simple mapper between User and Tarantool tuples from 'users' space.
 * This example shows, how to execute data managements operations, e.g: select, get,
 * insert, update, upsert, replace, delete.
 *
 * @author: Ivan Bannikov
 */
public class UsersStorage {

    /**
     * Simple modeul User
     */
    public static class User {
        int id;
        String name;
        Integer age;

        /**
         * Basic constructor for User class
         * @param id an id representing unique User
         * @param name a string, containing name of user
         * @param age non-negative integer representing age of user
         */
        public User(int id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "User(id=" + id + ", name='" + name + ", age=" + age + "')";
        }
    }

    private final TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space;
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(
            DefaultMessagePackMapperFactory
                .getInstance()
                .defaultComplexTypesMapper()
        );

    /**
     * Basic constructor for UsersStorage mapper.
     * This constructor accepts space operations objects.
     * Each data accessor and mutator method of this class uses methods from
     * this object.
     * 
     * @param space a TarantoolSpaceOperations&lt;TarantoolTuple, TarantoolResult&lt;TarantoolTuple&gt;&gt; object,
     * providing operations for Tarantool space
     * @see TarantoolSpaceOperations
     */
    public UsersStorage(TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space) {
        this.space = space;
    }

    /**
     * Gets a single record from users space.
     * To get a single tuple from space you should pass a special Conditions
     * object to `select` method. This object describes which tuple should be
     * retireved. Conditions object in this case contains only one predicate,
     * constructed by Conditions.equals(0, i), which talks to space operations
     * provider, that it have to select all tuples, which first field equal
     * some number. When operation is completed we got a TarantoolTuple object
     * providing methods for accesing to fields. Each fields of tuple can be
     * accessed by index (unlike Lua, fields in Java are enumerated from zero),
     * or by name.
     * @return CompletableFuture&lt;User&gt; object.
     */
    public CompletableFuture<User> get(int i) {
        return this.space
            .select(Conditions.equals(0, i))
            .thenApply(result -> {
                if (result.isEmpty()) {
                    return null;
                }
                TarantoolTuple user = result.get(0);
                return new User(
                    user.getInteger(0),
                    user.getString("name"),
                    user.getInteger("age")
                );
            });
    }

    /**
     * Inserts a tuple with user data into `users` space.
     * To construct tuple for inserting into space we use TarantoolTupleFactory, in this example
     * we have created factory with default types mapper.
     * @return CompletableFuture&lt;Boolean&gt; object.
     */
    public CompletableFuture<Boolean> insert(int i, String name, Integer age) {
        return this.space
            .insert(tupleFactory.create(i, name, age))
            .thenApply(result -> {
                if (result.isEmpty()) {
                    return false;
                }
                return true;
            });
    }

    /**
     * Deletes a tuple with user data from `users` space.
     * To delete a single tuple Conditions object should be also used.
     * Condition is the same, as for getting single record.
     * @return CompletableFuture&lt;Boolean&gt; object.
     */
    public CompletableFuture<Boolean> delete(int i) {
        return this.space
            .delete(Conditions.equals(0, i))
            .thenApply(result -> {
                if (result.isEmpty()) {
                    return false;
                }
                return true;
            });
    }

    /**
     * Updates a tuple in `users` space.
     * To delete a single tuple Conditions object should be also used.
     * Condition is the same, as for getting single record.
     * @return CompletableFuture&lt;Boolean&gt; object.
     */
    public CompletableFuture<User> update(int i, String newName) {
        return this.space
            .update(
                Conditions.equals(0, i),
                tupleFactory.create(i, newName)
            )
            .thenApply(result -> {
                if (result.isEmpty()) {
                    return null;
                }
                TarantoolTuple user = result.get(0);
                return new User(
                    user.getInteger(0),
                    user.getString("name"),
                    user.getInteger("age")
                );
            });
    }
}
