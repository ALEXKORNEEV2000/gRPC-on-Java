package org.example;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.TarantoolResponse;
import io.tarantool.mapping.Tuple;
import org.example.KVEntry;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class KVRepository {

    private static final int RANGE_BATCH_SIZE = 10_000;

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;

    public KVRepository() {
        this.client = TarantoolConnection.getClient();
        this.space = client.space("KV");
    }

    public void put(String key, byte[] value, boolean hasValue) {
        // hasValue=false -> сохраняем null в поле value
        List<Object> tuple = Arrays.asList(key, hasValue ? value : null);
        space.replace(tuple).join();
    }

    public KVEntry get(String key) {
        SelectResponse<List<Tuple<List<?>>>> resp = space
                .select(Collections.singletonList(key), SelectOptions.builder().withLimit(1).build())
                .join();

        if (resp.get().isEmpty()) {
            return null;
        }

        List<?> row = resp.get().get(0).get();
        byte[] value = toBytes(row.size() > 1 ? row.get(1) : null);
        return new KVEntry(key, value, value != null);
    }

    public boolean delete(String key) {
        Tuple<List<?>> deleted = space.delete(Collections.singletonList(key)).join();
        return deleted != null && deleted.get() != null && !deleted.get().isEmpty();
    }

    public long count() {
        TarantoolResponse<List<Long>> resp = client
                .eval("return box.space.KV:count()", Long.class)
                .join();

        if (resp.get().isEmpty()) {
            return 0L;
        }
        return resp.get().get(0);
    }

    public void range(String keySince, String keyTo, Consumer<KVEntry> consumer) {
        if (keySince.compareTo(keyTo) > 0) {
            return;
        }

        byte[] after = null;
        while (true) {
            SelectOptions.Builder options = SelectOptions.builder()
                    .withIterator(BoxIterator.GE)
                    .withLimit(RANGE_BATCH_SIZE)
                    .fetchPosition();

            if (after != null) {
                options.after(after);
            }

            SelectResponse<List<Tuple<List<?>>>> resp = space
                    .select(Collections.singletonList(keySince), options.build())
                    .join();

            List<Tuple<List<?>>> rows = resp.get();
            if (rows.isEmpty()) {
                return;
            }

            boolean stop = false;
            for (Tuple<List<?>> t : rows) {
                List<?> row = t.get();
                String key = String.valueOf(row.get(0));
                if (key.compareTo(keyTo) > 0) {
                    stop = true;
                    break;
                }

                byte[] value = toBytes(row.size() > 1 ? row.get(1) : null);
                consumer.accept(new KVEntry(key, value, value != null));
            }

            if (stop || resp.getPosition() == null) {
                return;
            }
            after = resp.getPosition();
        }
    }

    private byte[] toBytes(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof byte[] b) {
            return b;
        }
        if (raw instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unsupported Tarantool value type: " + raw.getClass());
    }
}