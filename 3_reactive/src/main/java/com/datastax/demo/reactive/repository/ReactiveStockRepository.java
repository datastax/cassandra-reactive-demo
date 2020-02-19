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
package com.datastax.demo.reactive.repository;

import com.datastax.demo.common.model.Stock;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.time.Instant;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** A DAO that manages the persistence of {@link Stock} instances. */
@Repository
@Profile("!unit-test")
public class ReactiveStockRepository {

  private final CqlSession session;

  private final PreparedStatement insert;
  private final PreparedStatement deleteById;
  private final PreparedStatement findById;
  private final PreparedStatement findBySymbol;
  private final Function<Row, Stock> rowMapper;

  public ReactiveStockRepository(
      CqlSession session,
      @Qualifier("stocks.prepared.insert") PreparedStatement insert,
      @Qualifier("stocks.prepared.deleteById") PreparedStatement deleteById,
      @Qualifier("stocks.prepared.findById") PreparedStatement findById,
      @Qualifier("stocks.prepared.findBySymbol") PreparedStatement findBySymbol,
      Function<Row, Stock> rowMapper) {
    this.session = session;
    this.insert = insert;
    this.deleteById = deleteById;
    this.findById = findById;
    this.findBySymbol = findBySymbol;
    this.rowMapper = rowMapper;
  }

  /**
   * Saves the given stock value.
   *
   * @param stock The stock value to save.
   * @return A {@link Mono} that will emit the saved stock value, then complete.
   */
  @NonNull
  public Mono<Stock> save(@NonNull Stock stock) {
    BoundStatement bound = insert.bind(stock.getSymbol(), stock.getDate(), stock.getValue());
    ReactiveResultSet rs = session.executeReactive(bound);
    return Mono.fromDirect(rs).then(Mono.just(stock));
  }

  /**
   * Deletes the stock value for the given symbol and date.
   *
   * @param symbol The stock symbol to delete.
   * @param date The stock date to delete.
   * @return An empty {@link Mono} that will complete when the operation is completed.
   */
  @NonNull
  public Mono<Void> deleteById(@NonNull String symbol, @NonNull Instant date) {
    BoundStatement bound = deleteById.bind(symbol, date);
    ReactiveResultSet rs = session.executeReactive(bound);
    return Mono.fromDirect(rs).then();
  }

  /**
   * Retrieves the stock value uniquely identified by its symbol and date.
   *
   * @param symbol The stock symbol to find.
   * @param date The stock date to find.
   * @return A {@link Mono} that will emit the retrieved stock value, if found, then complete.
   */
  @NonNull
  public Mono<Stock> findById(@NonNull String symbol, @NonNull Instant date) {
    BoundStatement bound = findById.bind(symbol, date);
    ReactiveResultSet rs = session.executeReactive(bound);
    return Mono.fromDirect(rs).map(rowMapper);
  }

  /**
   * Retrieves all the stock values for a given symbol in a given date range, page by page.
   *
   * @param symbol The stock symbol to find.
   * @param start The date range start (inclusive).
   * @param end The date range end (exclusive).
   * @param offset The zero-based index of the first result to return.
   * @param limit The maximum number of results to return.
   * @return A {@link Flux} that will emit the retrieved stocks, then complete.
   */
  @NonNull
  public Flux<Stock> findAllBySymbol(
      @NonNull String symbol,
      @NonNull Instant start,
      @NonNull Instant end,
      long offset,
      long limit) {
    BoundStatement bound = findBySymbol.bind(symbol, start, end);
    ReactiveResultSet rs = session.executeReactive(bound);
    Flux<ReactiveRow> flux = Flux.from(rs);
    return flux.skip(offset).take(limit).map(rowMapper);
  }
}
