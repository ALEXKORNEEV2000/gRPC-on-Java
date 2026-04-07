package org.example;

import org.example.model.KVEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Testcontainers
public class KVRepositoryTest {

    @Container
    static final GenericContainer<?> TARANTOOL =
            new GenericContainer<>("tarantool/tarantool:3.2")
                    .withExposedPorts(3301)
                    .withCommand("tarantool", "-e",
                            "box.cfg{listen=3301}; " +
                                    "box.schema.user.create('app',{password='app', if_not_exists=true}); " +
                                    "box.schema.user.grant('app','read,write,execute','universe',nil,{if_not_exists=true}); " +
                                    "local s=box.schema.space.create('KV',{if_not_exists=true}); " +
                                    "s:format({{name='key',type='string'},{name='value',type='varbinary',is_nullable=true}}); " +
                                    "s:create_index('primary',{type='TREE',parts={{field='key',type='string'}},if_not_exists=true}); " +
                                    "require('fiber').sleep(3600)");

    private static KVRepository repository;

    @BeforeEach
    void setUp() {
        System.setProperty("TARANTOOL_HOST", TARANTOOL.getHost());
        System.setProperty("TARANTOOL_PORT", String.valueOf(TARANTOOL.getMappedPort(3301)));
        System.setProperty("TARANTOOL_USER", "app");
        System.setProperty("TARANTOOL_PASSWORD", "app");

        // Т.к. ваш TarantoolConnection читает env, а не System properties,
        // лучше заменить env-использование на property-then-env.
        // Если не хотите менять код, задавайте env через IDE Run Configuration.
        repository = new KVRepository();

        // очистка перед каждым тестом
        TarantoolConnection.getClient().eval("box.space.KV:truncate()").join();
    }

    @AfterAll
    static void tearDown() {
        TarantoolConnection.close();
    }

    @Test
    void putGet_nullValue() {
        repository.put("k1", null, false);

        KVEntry got = repository.get("k1");

        Assertions.assertNotNull(got);
        Assertions.assertEquals("k1", got.key());
        Assertions.assertFalse(got.hasValue());
        Assertions.assertNull(got.value());
    }

    @Test
    void putGet_emptyBytes() {
        repository.put("k2", new byte[0], true);

        KVEntry got = repository.get("k2");

        Assertions.assertNotNull(got);
        Assertions.assertEquals("k2", got.key());
        Assertions.assertTrue(got.hasValue());
        Assertions.assertNotNull(got.value());
        Assertions.assertEquals(0, got.value().length);
    }

    @Test
    void range_boundaries() {
        repository.put("a", "1".getBytes(), true);
        repository.put("b", "2".getBytes(), true);
        repository.put("c", "3".getBytes(), true);
        repository.put("d", "4".getBytes(), true);

        List<String> keys = new ArrayList<>();
        repository.range("b", "c", e -> keys.add(e.key()));

        Assertions.assertEquals(Arrays.asList("b", "c"), keys);
    }

    @Test
    void count_and_delete() {
        repository.put("x1", "v1".getBytes(), true);
        repository.put("x2", "v2".getBytes(), true);
        repository.put("x3", null, false);

        Assertions.assertEquals(3L, repository.count());

        boolean deletedExisting = repository.delete("x2");
        boolean deletedMissing = repository.delete("x999");

        Assertions.assertTrue(deletedExisting);
        Assertions.assertFalse(deletedMissing);
        Assertions.assertEquals(2L, repository.count());
    }
}