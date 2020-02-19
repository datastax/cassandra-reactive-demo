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
package com.datastax.demo.common.conf;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;

/**
 * A configuration class that exposes CQL statements for the stocks table as beans.
 *
 * <p>This component exposes a few {@link SimpleStatement}s that perform both DDL and DML operations
 * on the stocks table, as well as a few {@link PreparedStatement}s for common DML operations.
 */
@Configuration
@Profile("!unit-test")
public class StockQueriesConfiguration {

  private static final CqlIdentifier STOCKS = CqlIdentifier.fromCql("stocks");

  public static final CqlIdentifier SYMBOL = CqlIdentifier.fromCql("symbol");
  public static final CqlIdentifier DATE = CqlIdentifier.fromCql("date");
  public static final CqlIdentifier VALUE = CqlIdentifier.fromCql("value");

  private static final CqlIdentifier START = CqlIdentifier.fromCql("start");
  private static final CqlIdentifier END = CqlIdentifier.fromCql("end");

  @Bean("stocks.simple.create")
  public SimpleStatement createTable(@NonNull CqlIdentifier keyspace) {
    return SchemaBuilder.createTable(keyspace, STOCKS)
        .ifNotExists()
        .withPartitionKey(SYMBOL, DataTypes.TEXT)
        .withClusteringColumn(DATE, DataTypes.TIMESTAMP)
        .withColumn(VALUE, DataTypes.DECIMAL)
        .withClusteringOrder(DATE, ClusteringOrder.DESC)
        .build();
  }

  @Bean("stocks.simple.drop")
  public SimpleStatement dropTable(@NonNull CqlIdentifier keyspace) {
    return SchemaBuilder.dropTable(keyspace, STOCKS).ifExists().build();
  }

  @Bean("stocks.simple.truncate")
  public SimpleStatement truncate(@NonNull CqlIdentifier keyspace) {
    return QueryBuilder.truncate(keyspace, STOCKS).build();
  }

  @Bean("stocks.simple.insert")
  public SimpleStatement insert(@NonNull CqlIdentifier keyspace) {
    return QueryBuilder.insertInto(keyspace, STOCKS)
        .value(SYMBOL, bindMarker(SYMBOL))
        .value(DATE, bindMarker(DATE))
        .value(VALUE, bindMarker(VALUE))
        .build();
  }

  @Bean("stocks.simple.deleteById")
  public SimpleStatement deleteById(@NonNull CqlIdentifier keyspace) {
    return QueryBuilder.deleteFrom(keyspace, STOCKS)
        .where(column(SYMBOL).isEqualTo(bindMarker(SYMBOL)))
        .where(column(DATE).isEqualTo(bindMarker(DATE)))
        .build();
  }

  @Bean("stocks.simple.findById")
  public SimpleStatement findById(@NonNull CqlIdentifier keyspace) {
    return QueryBuilder.selectFrom(keyspace, STOCKS)
        .columns(SYMBOL, DATE, VALUE)
        .where(column(SYMBOL).isEqualTo(bindMarker(SYMBOL)))
        .where(column(DATE).isEqualTo(bindMarker(DATE)))
        .build();
  }

  @Bean("stocks.simple.findBySymbol")
  public SimpleStatement findBySymbol(@NonNull CqlIdentifier keyspace) {
    return QueryBuilder.selectFrom(keyspace, STOCKS)
        .columns(SYMBOL, DATE, VALUE)
        .where(
            Relation.column(SYMBOL).isEqualTo(bindMarker(SYMBOL)),
            // start inclusive
            Relation.column(DATE).isGreaterThanOrEqualTo(bindMarker(START)),
            // end exclusive
            Relation.column(DATE).isLessThan(bindMarker(END)))
        .build();
  }

  @Bean("stocks.prepared.insert")
  public PreparedStatement prepareInsert(
      CqlSession session, @Qualifier("stocks.simple.insert") SimpleStatement stockInsert) {
    return session.prepare(stockInsert);
  }

  @Bean("stocks.prepared.deleteById")
  public PreparedStatement prepareDeleteById(
      CqlSession session, @Qualifier("stocks.simple.deleteById") SimpleStatement stockDeleteById) {
    return session.prepare(stockDeleteById);
  }

  @Bean("stocks.prepared.findById")
  public PreparedStatement prepareFindById(
      CqlSession session, @Qualifier("stocks.simple.findById") SimpleStatement stockFindById) {
    return session.prepare(stockFindById);
  }

  @Bean("stocks.prepared.findBySymbol")
  public PreparedStatement prepareFindBySymbol(
      CqlSession session,
      @Qualifier("stocks.simple.findBySymbol") SimpleStatement stockFindBySymbol) {
    return session.prepare(stockFindBySymbol);
  }
}
