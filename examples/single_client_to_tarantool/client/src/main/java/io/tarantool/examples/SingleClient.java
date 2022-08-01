package io.tarantool.examples;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import static io.tarantool.examples.UsersStorage.User;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Single Client implementation
 */
public class SingleClient  {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3301;
    private static final String USER = "authenticator";
    private static final String PASSWORD = "secret";
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(
            DefaultMessagePackMapperFactory
                .getInstance()
                .defaultComplexTypesMapper()
        );
    private static final SimpleTarantoolCredentials credentials =
        new SimpleTarantoolCredentials(USER, PASSWORD);

    public static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> createClient() {
        return TarantoolClientFactory.createClient()
            .withAddress(HOST, PORT)
            .withCredentials(credentials)
            .build();
    }

    public static void main(String[] args) throws Exception {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = createClient();
        UsersStorage users = new UsersStorage(client.space("users"));
        String command = args[0];
        int userId = Integer.parseInt(args[1]);
        String userName;
        Boolean ok;
        User result;
        switch (command) {
            case "get":
                result = users.get(userId).get();
                System.out.println("selected user: " + result);
                break;
            case "insert":
                userName = args[2];
                ok = users.insert(userId, userName).get();
                if (ok) {
                    System.out.println("user inserted");
                } else {
                    System.out.println("user is not inserted");
                }
                break;
            case "delete":
                ok = users.delete(userId).get();
                if (ok) {
                    System.out.println("user deleted");
                } else {
                    System.out.println("user is not deleted");
                }
                break;
            case "update":
                userName = args[2];
                result = users.update(userId, userName).get();
                System.out.println("update result: " + result);
                break;
            default:
                System.out.println("Please, use one of following commands: " +
                                   "get, insert, delete or update");
        }
        client.close();
    }
}
