package org.example;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;

import java.nio.charset.StandardCharsets;

/**
 * Singleton connection to Tarantool using tarantool-client 1.5.0.
 */
public final class TarantoolConnection {

    private static volatile TarantoolBoxClient client;

    private static final String HOST = env("TARANTOOL_HOST", "127.0.0.1");
    private static final int PORT = Integer.parseInt(env("TARANTOOL_PORT", "3301"));
    private static final String USER = env("TARANTOOL_USER", "guest");
    private static final String PASSWORD = env("TARANTOOL_PASSWORD", "");

    private TarantoolConnection() {
    }

    public static TarantoolBoxClient getClient() {
        TarantoolBoxClient local = client;
        if (local == null) {
            synchronized (TarantoolConnection.class) {
                local = client;
                if (local == null) {
                    try {
                        local = TarantoolFactory.box()
                                .withHost(HOST)
                                .withPort(PORT)
                                .withUser(USER)
                                .withPassword(PASSWORD)
                                .build();

                        initSpace(local);
                        client = local;
                        System.out.println("Tarantool connected: " + HOST + ":" + PORT);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize Tarantool connection", e);
                    }
                }
            }
        }
        return local;
    }

    /**
     * Idempotent initialization of KV space:
     * { key:string, value:varbinary(nullable) } + primary TREE index by key.
     */
    private static void initSpace(TarantoolBoxClient c) {
        // Create space if missing
        c.eval("box.schema.space.create('KV', {if_not_exists = true})").join();

        // Set format
        c.eval(
                "box.space.KV:format({" +
                        "{name='key', type='string'}," +
                        "{name='value', type='varbinary', is_nullable=true}" +
                        "})"
        ).join();

        // Ensure primary index
        c.eval(
                "box.space.KV:create_index('primary', {" +
                        "type='TREE'," +
                        "parts={{field='key', type='string'}}," +
                        "if_not_exists=true" +
                        "})"
        ).join();

        // Optional tiny sanity probe
        c.eval("return box.space.KV ~= nil").join();
    }

    public static void close() {
        TarantoolBoxClient local = client;
        if (local != null) {
            synchronized (TarantoolConnection.class) {
                local = client;
                if (local != null) {
                    try {
                        local.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to close Tarantool client", e);
                    } finally {
                        client = null;
                    }
                }
            }
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}