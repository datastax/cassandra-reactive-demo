/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.demo.sync.repository;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;
import static java.time.ZoneOffset.UTC;

import com.datastax.demo.sync.model.Stock;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import java.time.Instant;
import java.time.Period;
import java.util.Objects;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

public final class StockQueries {

  private static final CqlIdentifier STOCKS = CqlIdentifier.fromCql("stocks");

  private static final CqlIdentifier SYMBOL = CqlIdentifier.fromCql("symbol");
  private static final CqlIdentifier DATE = CqlIdentifier.fromCql("date");
  private static final CqlIdentifier VALUE = CqlIdentifier.fromCql("value");

  private static final CqlIdentifier START = CqlIdentifier.fromCql("start");
  private static final CqlIdentifier END = CqlIdentifier.fromCql("end");

  private static final Instant MIN_START = Instant.EPOCH;
  private static final Instant MAX_END =
      MIN_START.atZone(UTC).plus(Period.ofYears(100)).toInstant();

  @NonNull
  public static SimpleStatement createTable(@Nullable CqlIdentifier keyspace) {
    return SchemaBuilder.createTable(keyspace, STOCKS)
        .ifNotExists()
        .withPartitionKey(SYMBOL, DataTypes.TEXT)
        .withClusteringColumn(DATE, DataTypes.TIMESTAMP)
        .withColumn(VALUE, DataTypes.DOUBLE)
        .withClusteringOrder(DATE, ClusteringOrder.DESC)
        .build();
  }

  @NonNull
  public static SimpleStatement dropTable(@Nullable CqlIdentifier keyspace) {
    return SchemaBuilder.dropTable(keyspace, STOCKS).ifExists().build();
  }

  @NonNull
  public static SimpleStatement insert(@Nullable CqlIdentifier keyspace) {
    return QueryBuilder.insertInto(keyspace, STOCKS)
        .value(SYMBOL, bindMarker(SYMBOL))
        .value(DATE, bindMarker(DATE))
        .value(VALUE, bindMarker(VALUE))
        .build();
  }

  @NonNull
  static SimpleStatement deleteById(@Nullable CqlIdentifier keyspace) {
    return QueryBuilder.deleteFrom(keyspace, STOCKS)
        .where(column(SYMBOL).isEqualTo(bindMarker(SYMBOL)))
        .where(column(DATE).isEqualTo(bindMarker(DATE)))
        .build();
  }

  @NonNull
  static SimpleStatement findBySymbol(@Nullable CqlIdentifier keyspace) {
    return QueryBuilder.selectFrom(keyspace, STOCKS)
        .all()
        .where(
            Relation.column(SYMBOL).isEqualTo(bindMarker(SYMBOL)),
            // start inclusive
            Relation.column(DATE).isGreaterThanOrEqualTo(bindMarker(START)),
            // end exclusive
            Relation.column(DATE).isLessThan(bindMarker(END)))
        .build();
  }

  @NonNull
  static SimpleStatement findById(@Nullable CqlIdentifier keyspace) {
    return QueryBuilder.selectFrom(keyspace, STOCKS)
        .all()
        .where(column(SYMBOL).isEqualTo(bindMarker(SYMBOL)))
        .where(column(DATE).isEqualTo(bindMarker(DATE)))
        .build();
  }

  @NonNull
  public static BoundStatement bind(@NonNull PreparedStatement ps, @NonNull Stock stock) {
    return ps.boundStatementBuilder()
        .setString(SYMBOL, stock.getSymbol())
        .setInstant(DATE, stock.getDate())
        .setDouble(VALUE, stock.getValue())
        .build();
  }

  @NonNull
  static BoundStatement bindRange(
      PreparedStatement ps,
      @NonNull String symbol,
      @Nullable Instant start,
      @Nullable Instant end) {
    return ps.boundStatementBuilder()
        .setString(SYMBOL, symbol)
        .setInstant(START, start == null ? MIN_START : start)
        .setInstant(END, end == null ? MAX_END : end)
        .build();
  }

  @NonNull
  static Stock map(@NonNull Row row) {
    String symbol = Objects.requireNonNull(row.getString(SYMBOL));
    Instant date = Objects.requireNonNull(row.getInstant(DATE));
    double value = row.getDouble(VALUE);
    return new Stock(symbol, date, value);
  }
}
