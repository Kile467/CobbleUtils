package com.kingpixel.cobbleutils.util.sql;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Generic SQL storage for one JSON document per row (key column + payload column).
 * <p>
 * Intended for mods that persist player or entity state as JSON (e.g. battle pass progress).
 */
public final class JsonDocumentStore {

  private final SQLManager manager;
  private final String table;
  private final String keyColumn;
  private final String payloadColumn;
  private final String createTableSql;

  public JsonDocumentStore(
    @NonNull SQLManager manager,
    @NonNull String table,
    @NonNull String keyColumn,
    @NonNull String payloadColumn,
    @NonNull String createTableSql
  ) {
    this.manager = Objects.requireNonNull(manager, "manager");
    this.table = validateIdentifier(table, "table");
    this.keyColumn = validateIdentifier(keyColumn, "keyColumn");
    this.payloadColumn = validateIdentifier(payloadColumn, "payloadColumn");
    this.createTableSql = Objects.requireNonNull(createTableSql, "createTableSql");
  }

  private static String validateIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
      throw new IllegalArgumentException("Invalid SQL identifier for " + name + ": " + value);
    }
    return value;
  }

  public void createTableIfNeeded() {
    manager.execute(createTableSql);
  }

  public CompletableFuture<String> findPayload(@NonNull String key) {
    String sql = "SELECT " + payloadColumn + " FROM " + table + " WHERE " + keyColumn + " = ?";
    return manager.queryAsync(sql, rs -> rs.next() ? rs.getString(payloadColumn) : null, key);
  }

  public CompletableFuture<Void> upsertPayload(@NonNull String key, @NonNull String json) {
    String sql = """
      INSERT INTO %s (%s, %s) VALUES (?, CAST(? AS JSON))
      ON DUPLICATE KEY UPDATE %s = CAST(? AS JSON)
      """.formatted(table, keyColumn, payloadColumn, payloadColumn);
    return manager.runAsync(() -> manager.execute(sql, key, json, json));
  }

  /**
   * Runs a custom SELECT that returns payload strings from the first column of each row.
   */
  public CompletableFuture<List<String>> queryPayloadList(@NonNull String sql, Object... params) {
    return manager.queryListAsync(sql, rs -> rs.getString(1), params)
      .thenApply(list -> list == null ? List.of() : new ArrayList<>(list));
  }

  /**
   * Inserts a new row with {@code seedPayload} or increments a numeric JSON path on existing rows (MySQL/MariaDB).
   */
  public CompletableFuture<Void> upsertAndIncrementJsonPath(
    @NonNull String key,
    @NonNull String seedPayload,
    @NonNull String jsonPath,
    double increment
  ) {
    Objects.requireNonNull(jsonPath, "jsonPath");
    if (!jsonPath.startsWith("$.")) {
      throw new IllegalArgumentException("jsonPath must start with $. (got: " + jsonPath + ")");
    }

    String sql = """
      INSERT INTO %s (%s, %s) VALUES (?, CAST(? AS JSON))
      ON DUPLICATE KEY UPDATE %s = JSON_SET(
        %s,
        ?,
        COALESCE(JSON_EXTRACT(%s, ?), 0) + ?
      )
      """.formatted(table, keyColumn, payloadColumn, payloadColumn, payloadColumn, payloadColumn);

    return manager.runAsync(() -> manager.execute(
      sql,
      key,
      seedPayload,
      jsonPath,
      jsonPath,
      increment
    ));
  }
}
