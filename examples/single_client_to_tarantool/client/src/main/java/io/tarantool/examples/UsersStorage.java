package io.tarantool.examples;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;

import java.util.concurrent.CompletableFuture;


public class UsersStorage {
    public static class User {
        int id;
        String name;

        public User(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "User(id=" + id + ", name='" + name + "')";
        }
    }

    private final TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space;
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(
            DefaultMessagePackMapperFactory
                .getInstance()
                .defaultComplexTypesMapper()
        );

    public UsersStorage(TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space) {
        this.space = space;
    }

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
                    user.getString("name")
                );
            });
    }

    public CompletableFuture<Boolean> insert(int i, String name) {
        return this.space
            .insert(tupleFactory.create(i, name))
            .thenApply(result -> {
                if (result.isEmpty()) {
                    return false;
                }
                return true;
            });
    }

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
                    user.getString("name")
                );
            });
    }
}
