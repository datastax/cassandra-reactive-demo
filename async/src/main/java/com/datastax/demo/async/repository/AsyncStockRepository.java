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

import com.datastax.demo.common.dto.PagedResults;
import com.datastax.demo.common.model.Stock;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

/** A DAO that manages the persistence of {@link Stock} instances. */
@Repository
@Profile("!unit-test")
public class AsyncStockRepository {

  private final DseSession session;

  private final PreparedStatement insert;
  private final PreparedStatement deleteById;
  private final PreparedStatement findById;
  private final PreparedStatement findBySymbol;

  public AsyncStockRepository(
      DseSession session,
      @Qualifier("stocks.prepared.insert") PreparedStatement insert,
      @Qualifier("stocks.prepared.deleteById") PreparedStatement deleteById,
      @Qualifier("stocks.prepared.findById") PreparedStatement findById,
      @Qualifier("stocks.prepared.findBySymbol") PreparedStatement findBySymbol) {
    this.session = session;
    this.insert = insert;
    this.deleteById = deleteById;
    this.findById = findById;
    this.findBySymbol = findBySymbol;
  }

  /**
   * Saves the given stock value.
   *
   * @param stock The stock value to save.
   * @return A future that will complete when the operation is completed.
   */
  @NonNull
  public CompletionStage<Stock> save(@NonNull Stock stock) {
    BoundStatement bound = insert.bind(stock.getSymbol(), stock.getDate(), stock.getValue());
    return session.executeAsync(bound).thenApply(rs -> stock);
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
    return session.executeAsync(bound).thenApply(rs -> null);
  }

  /**
   * Retrieves the stock value uniquely identified by its symbol and date.
   *
   * @param symbol The stock symbol to find.
   * @param date The stock date to find.
   * @return A future that will complete when the operation is completed with the retrieved stock
   *     value, or empty if not found.
   */
  @NonNull
  public CompletionStage<Optional<Stock>> findById(@NonNull String symbol, @NonNull Instant date) {
    BoundStatement bound = findById.bind(symbol, date);
    return session
        .executeAsync(bound)
        .thenApply(AsyncPagingIterable::one)
        .thenApply(row -> Optional.ofNullable(row).map(this::map));
  }

  /**
   * Retrieves all the stock values for a given symbol in a given date range, page by page.
   *
   * @param symbol The stock symbol to find.
   * @param startInclusive The date range start (inclusive).
   * @param endExclusive The date range end (exclusive).
   * @param pagingState The paging state, or {@code null} to retrieve the first page.
   * @return A future that will complete when the operation is completed with a page of results.
   */
  @NonNull
  public CompletionStage<PagedResults<Stock>> findAllBySymbol(
      @NonNull String symbol,
      @NonNull Instant startInclusive,
      @NonNull Instant endExclusive,
      @Nullable ByteBuffer pagingState) {
    BoundStatement bound =
        findBySymbol.bind(symbol, startInclusive, endExclusive).setPagingState(pagingState);
    return session
        .executeAsync(bound)
        .thenApply(
            rs -> {
              Stream<Stock> results =
                  StreamSupport.stream(rs.currentPage().spliterator(), false).map(this::map);
              ByteBuffer nextPage = rs.getExecutionInfo().getPagingState();
              return new PagedResults<>(results, nextPage);
            });
  }

  @NonNull
  private Stock map(@NonNull Row row) {
    var symbol = Objects.requireNonNull(row.getString(0));
    var date = Objects.requireNonNull(row.getInstant(1));
    var value = Objects.requireNonNull(row.getBigDecimal(2));
    return new Stock(symbol, date, value);
  }
}
