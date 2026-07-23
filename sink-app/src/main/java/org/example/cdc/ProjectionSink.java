package org.example.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;

import javax.cache.Cache;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public final class ProjectionSink {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CACHE_NAME = "orders_projection";

    private final String topic = env("KAFKA_TOPIC", "oracle.RETAIL.ORDERS");
    private final String postgresUrl = env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/cdc");
    private final String postgresUser = env("POSTGRES_USER", "cdc");
    private final String postgresPassword = env("POSTGRES_PASSWORD", "PostgresPwd123");
    private final String igniteAddress = env("IGNITE_ADDRESS", "ignite:10800");
    private final String adminToken = env("ADMIN_TOKEN", "sandbox");

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicateOrStale = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final List<Long> latencySamples = Collections.synchronizedList(new ArrayList<>());
    private final long startedAt = System.currentTimeMillis();

    private volatile Connection postgres;
    private volatile IgniteClient ignite;
    private volatile ClientCache<Long, String> cache;

    public static void main(String[] args) throws Exception {
        new ProjectionSink().run();
    }

    private void run() throws Exception {
        connectTargetsWithRetry();
        startHttpServer();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, env("SINK_GROUP_ID", "cdc-projection-sink"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            System.out.printf("Projection sink subscribed to %s%n", topic);
            while (!Thread.currentThread().isInterrupted()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    processWithRetry(record);
                }
                consumer.commitSync();
            }
        } finally {
            closeTargets();
        }
    }

    private void processWithRetry(ConsumerRecord<String, String> record) throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                process(record);
                return;
            } catch (Exception exception) {
                failures.incrementAndGet();
                System.err.printf("Processing failed at offset %d: %s%n", record.offset(), exception.getMessage());
                reconnectTargets();
                Thread.sleep(2000);
            }
        }
    }

    private void process(ConsumerRecord<String, String> record) throws Exception {
        consumed.incrementAndGet();
        if (record.value() == null || record.value().isBlank()) {
            return; // Kafka tombstone; the preceding Debezium delete envelope already carries the key.
        }

        JsonNode envelope = MAPPER.readTree(record.value());
        String operation = envelope.path("op").asText("");
        if (operation.isBlank()) {
            return;
        }

        JsonNode before = envelope.path("before");
        JsonNode after = envelope.path("after");
        boolean deleted = "d".equals(operation);
        JsonNode state = deleted ? before : after;
        if (state.isMissingNode() || state.isNull()) {
            return;
        }

        long orderId = asLong(state, "ORDER_ID");
        JsonNode source = envelope.path("source");
        long sourceTsMs = source.path("ts_ms").asLong(envelope.path("ts_ms").asLong(System.currentTimeMillis()));
        if (sourceTsMs <= 0) {
            sourceTsMs = envelope.path("ts_ms").asLong(System.currentTimeMillis());
        }
        BigDecimal sourceScn = scn(source);
        String payload = deleted ? null : MAPPER.writeValueAsString(state);

        boolean changed = upsertPostgres(orderId, state, deleted, sourceScn, record.partition(), record.offset(), sourceTsMs, payload);
        if (changed) {
            updateIgnite(orderId, state, deleted, sourceScn, record.partition(), record.offset(), sourceTsMs, payload);
            applied.incrementAndGet();
            if (deleted) {
                deletes.incrementAndGet();
            }
        } else {
            duplicateOrStale.incrementAndGet();
        }

        long latency = Math.max(0, System.currentTimeMillis() - sourceTsMs);
        synchronized (latencySamples) {
            if (latencySamples.size() >= 100_000) {
                latencySamples.remove(0);
            }
            latencySamples.add(latency);
        }
    }

    private boolean upsertPostgres(long orderId, JsonNode state, boolean deleted,
                                   BigDecimal sourceScn, int sourcePartition, long sourceOffset,
                                   long sourceTsMs, String payload) throws SQLException {
        ensurePostgres();
        String sql = """
            INSERT INTO orders_projection
              (order_id, customer_id, status, total_cents, version_no, is_deleted,
               source_scn, source_partition, source_offset, source_ts_ms, payload, applied_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CURRENT_TIMESTAMP)
            ON CONFLICT (order_id) DO UPDATE SET
              customer_id = EXCLUDED.customer_id,
              status = EXCLUDED.status,
              total_cents = EXCLUDED.total_cents,
              version_no = EXCLUDED.version_no,
              is_deleted = EXCLUDED.is_deleted,
              source_scn = EXCLUDED.source_scn,
              source_partition = EXCLUDED.source_partition,
              source_offset = EXCLUDED.source_offset,
              source_ts_ms = EXCLUDED.source_ts_ms,
              payload = EXCLUDED.payload,
              applied_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.source_scn > orders_projection.source_scn
               OR (EXCLUDED.source_scn = orders_projection.source_scn
                   AND EXCLUDED.source_partition = orders_projection.source_partition
                   AND EXCLUDED.source_offset > orders_projection.source_offset)
            """;
        try (PreparedStatement statement = postgres.prepareStatement(sql)) {
            statement.setLong(1, orderId);
            if (deleted) {
                statement.setNull(2, java.sql.Types.BIGINT);
                statement.setNull(3, java.sql.Types.VARCHAR);
                statement.setNull(4, java.sql.Types.BIGINT);
                statement.setNull(5, java.sql.Types.BIGINT);
            } else {
                statement.setLong(2, asLong(state, "CUSTOMER_ID"));
                statement.setString(3, state.path("STATUS").asText());
                statement.setLong(4, asLong(state, "TOTAL_CENTS"));
                statement.setLong(5, asLong(state, "VERSION_NO"));
            }
            statement.setBoolean(6, deleted);
            statement.setBigDecimal(7, sourceScn);
            statement.setInt(8, sourcePartition);
            statement.setLong(9, sourceOffset);
            statement.setLong(10, sourceTsMs);
            statement.setString(11, payload == null ? "null" : payload);
            int changed = statement.executeUpdate();
            postgres.commit();
            return changed > 0;
        } catch (SQLException exception) {
            rollbackQuietly();
            throw exception;
        }
    }

    private void updateIgnite(long orderId, JsonNode state, boolean deleted,
                              BigDecimal sourceScn, int sourcePartition, long sourceOffset,
                              long sourceTsMs, String payload) throws Exception {
        ensureIgnite();
        String current = cache.get(orderId);
        if (current != null) {
            JsonNode existing = MAPPER.readTree(current);
            BigDecimal currentScn = new BigDecimal(existing.path("sourceScn").asText("0"));
            int currentPartition = existing.path("sourcePartition").asInt(-1);
            long currentOffset = existing.path("sourceOffset").asLong(-1);
            int scnComparison = sourceScn.compareTo(currentScn);
            if (scnComparison < 0 ||
                (scnComparison == 0 && sourcePartition == currentPartition && sourceOffset <= currentOffset)) {
                return;
            }
        }
        String cacheValue = MAPPER.writeValueAsString(Map.of(
            "orderId", orderId,
            "deleted", deleted,
            "sourceScn", sourceScn.toPlainString(),
            "sourcePartition", sourcePartition,
            "sourceOffset", sourceOffset,
            "sourceTsMs", sourceTsMs,
            "payload", payload == null ? "" : payload
        ));
        cache.put(orderId, cacheValue);
    }

    private void startHttpServer() throws IOException {
        int port = Integer.parseInt(env("HTTP_PORT", "8080"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> sendJson(exchange, 200, "{\"status\":\"UP\"}"));
        server.createContext("/metrics", exchange -> sendJson(exchange, 200, metricsJson()));
        server.createContext("/admin/reset-metrics", exchange -> {
            if (!authorized(exchange)) {
                sendJson(exchange, 403, "{\"error\":\"forbidden\"}");
                return;
            }
            consumed.set(0); applied.set(0); duplicateOrStale.set(0); deletes.set(0); failures.set(0);
            synchronized (latencySamples) { latencySamples.clear(); }
            sendJson(exchange, 200, "{\"status\":\"reset\"}");
        });
        server.createContext("/admin/clear", exchange -> {
            if (!authorized(exchange)) {
                sendJson(exchange, 403, "{\"error\":\"forbidden\"}");
                return;
            }
            try {
                ensurePostgres();
                try (PreparedStatement statement = postgres.prepareStatement("TRUNCATE TABLE orders_projection")) {
                    statement.execute();
                    postgres.commit();
                }
                ensureIgnite();
                cache.clear();
                sendJson(exchange, 200, "{\"status\":\"cleared\"}");
            } catch (Exception exception) {
                sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", exception.getMessage())));
            }
        });
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    private String metricsJson() throws IOException {
        List<Long> sorted;
        synchronized (latencySamples) {
            sorted = new ArrayList<>(latencySamples);
        }
        Collections.sort(sorted);
        long cacheTotal = -1;
        long cacheActive = -1;
        try {
            ensureIgnite();
            cacheTotal = cache.size();
            long active = 0;
            try (var cursor = cache.query(new ScanQuery<Long, String>())) {
                for (Cache.Entry<Long, String> entry : cursor) {
                    try {
                        if (!MAPPER.readTree(entry.getValue()).path("deleted").asBoolean(true)) {
                            active++;
                        }
                    } catch (Exception ignored) {
                        // Ignore malformed cache entries in the active-count metric.
                    }
                }
            }
            cacheActive = active;
        } catch (Exception ignored) {
            // Metrics should remain available even during a target outage.
        }

        return MAPPER.writeValueAsString(Map.ofEntries(
            Map.entry("startedAt", Instant.ofEpochMilli(startedAt).toString()),
            Map.entry("consumed", consumed.get()),
            Map.entry("applied", applied.get()),
            Map.entry("duplicateOrStale", duplicateOrStale.get()),
            Map.entry("deletes", deletes.get()),
            Map.entry("failures", failures.get()),
            Map.entry("latencySamples", sorted.size()),
            Map.entry("latencyP50Ms", percentile(sorted, 0.50)),
            Map.entry("latencyP95Ms", percentile(sorted, 0.95)),
            Map.entry("latencyP99Ms", percentile(sorted, 0.99)),
            Map.entry("cacheTotalCount", cacheTotal),
            Map.entry("cacheActiveCount", cacheActive)
        ));
    }

    private static long percentile(List<Long> values, double percentile) {
        if (values.isEmpty()) return -1;
        int index = (int) Math.ceil(percentile * values.size()) - 1;
        return values.get(Math.max(0, Math.min(index, values.size() - 1)));
    }

    private boolean authorized(HttpExchange exchange) {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) return false;
        String token = exchange.getRequestHeaders().getFirst("X-Admin-Token");
        return adminToken.equals(token);
    }

    private static void sendJson(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private synchronized void connectTargetsWithRetry() throws InterruptedException {
        while (true) {
            try {
                connectPostgres();
                connectIgnite();
                return;
            } catch (Exception exception) {
                System.err.println("Waiting for target systems: " + exception.getMessage());
                closeTargets();
                Thread.sleep(3000);
            }
        }
    }

    private synchronized void reconnectTargets() {
        closeTargets();
        try {
            connectTargetsWithRetry();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void connectPostgres() throws SQLException {
        postgres = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
        postgres.setAutoCommit(false);
        try (PreparedStatement statement = postgres.prepareStatement("SELECT 1")) {
            statement.execute();
        }
    }

    private void connectIgnite() {
        ignite = Ignition.startClient(new ClientConfiguration().setAddresses(igniteAddress));
        cache = ignite.getOrCreateCache(CACHE_NAME);
    }

    private void ensurePostgres() throws SQLException {
        if (postgres == null || postgres.isClosed() || !postgres.isValid(2)) {
            connectPostgres();
        }
    }

    private void ensureIgnite() {
        if (ignite == null || cache == null) {
            connectIgnite();
        }
    }

    private synchronized void closeTargets() {
        if (postgres != null) {
            try { postgres.close(); } catch (SQLException ignored) { }
            postgres = null;
        }
        if (ignite != null) {
            try { ignite.close(); } catch (Exception ignored) { }
            ignite = null;
            cache = null;
        }
    }

    private void rollbackQuietly() {
        if (postgres != null) {
            try { postgres.rollback(); } catch (SQLException ignored) { }
        }
    }

    private static long asLong(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isNumber()) return value.asLong();
        return Long.parseLong(value.asText());
    }

    private static BigDecimal scn(JsonNode source) {
        String value = source.path("scn").asText("0");
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException ignored) {
            return BigDecimal.valueOf(source.path("ts_ms").asLong(System.currentTimeMillis()));
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
