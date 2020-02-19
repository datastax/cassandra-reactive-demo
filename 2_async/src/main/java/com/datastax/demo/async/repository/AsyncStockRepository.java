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
package com.datastax.demo.async.repository;

import com.datastax.demo.common.model.Stock;
import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;

/** A DAO that manages the persistence of {@link Stock} instances. */
@Repository
@Profile("!unit-test")
public class AsyncStockRepository {

  private final CqlSession session;

  private final PreparedStatement insert;
  private final PreparedStatement deleteById;
  private final PreparedStatement findById;
  private final PreparedStatement findBySymbol;
  private final Function<Row, Stock> rowMapper;

  public AsyncStockRepository(
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
   * @return A future that will complete with the saved stock value.
   */
  @NonNull
  public CompletionStage<Stock> save(@NonNull Stock stock) {
    BoundStatement bound = insert.bind(stock.getSymbol(), stock.getDate(), stock.getValue());
    CompletionStage<AsyncResultSet> stage = session.executeAsync(bound);
    return stage.thenApply(rs -> stock);
  }

  /**
   * Deletes the stock value for the given symbol and date.
   *
   * @param symbol The stock symbol to delete.
   * @param date The stock date to delete.
   * @return A future that will complete when the operation is completed.
   */
  @NonNull
  public CompletionStage<Void> deleteById(@NonNull String symbol, @NonNull Instant date) {
    BoundStatement bound = deleteById.bind(symbol, date);
    CompletionStage<AsyncResultSet> stage = session.executeAsync(bound);
    return stage.thenApply(rs -> null);
  }

  /**
   * Retrieves the stock value uniquely identified by its symbol and date.
   *
   * @param symbol The stock symbol to find.
   * @param date The stock date to find.
   * @return A future that will complete with the retrieved stock value, or empty if not found.
   */
  @NonNull
  public CompletionStage<Optional<Stock>> findById(@NonNull String symbol, @NonNull Instant date) {
    BoundStatement bound = findById.bind(symbol, date);
    CompletionStage<AsyncResultSet> stage = session.executeAsync(bound);
    return stage
        .thenApply(AsyncPagingIterable::one)
        .thenApply(Optional::ofNullable)
        .thenApply(optional -> optional.map(rowMapper));
  }

  /**
   * Retrieves all the stock values for a given symbol in a given date range, page by page.
   *
   * @param symbol The stock symbol to find.
   * @param start The date range start (inclusive).
   * @param end The date range end (exclusive).
   * @param offset The zero-based index of the first result to return.
   * @param limit The maximum number of results to return.
   * @return A future that will complete with a {@link Stream} of results.
   */
  @NonNull
  public CompletionStage<Stream<Stock>> findAllBySymbol(
      @NonNull String symbol,
      @NonNull Instant start,
      @NonNull Instant end,
      long offset,
      long limit) {
    BoundStatement bound = findBySymbol.bind(symbol, start, end);
    CompletionStage<AsyncResultSet> stage = session.executeAsync(bound);
    return stage
        .thenCompose(first -> new RowCollector(first, offset, limit))
        .thenApply(rows -> rows.stream().map(rowMapper));
  }

  private static class RowCollector extends CompletableFuture<List<Row>> {

    final List<Row> rows = new ArrayList<>();
    long offset;
    long limit;

    RowCollector(AsyncResultSet first, long offset, long limit) {
      this.offset = offset;
      this.limit = limit;
      consumePage(first);
    }

    void consumePage(AsyncResultSet page) {
      for (Row row : page.currentPage()) {
        if (offset > 0) {
          offset--;
        } else if (limit > 0) {
          rows.add(row);
          limit--;
        }
      }
      if (page.hasMorePages() && limit > 0) {
        page.fetchNextPage().thenAccept(this::consumePage);
      } else {
        complete(rows);
      }
    }
  }
}
