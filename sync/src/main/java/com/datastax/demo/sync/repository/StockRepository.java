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

import com.datastax.demo.sync.dto.QueryResults;
import com.datastax.demo.sync.model.Stock;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

@Repository
public class StockRepository {

  @Autowired private DseSession session;

  private PreparedStatement insert;
  private PreparedStatement deleteById;
  private PreparedStatement findById;
  private PreparedStatement findBySymbol;

  @PostConstruct
  public void prepareStatements() {
    insert = session.prepare(StockQueries.insert(null));
    deleteById = session.prepare(StockQueries.deleteById(null));
    findById = session.prepare(StockQueries.findById(null));
    findBySymbol = session.prepare(StockQueries.findBySymbol(null));
  }

  public void save(@NonNull Stock stock) {
    session.execute(StockQueries.bind(insert, stock));
  }

  public void deleteById(@NonNull String symbol, @NonNull Instant date) {
    session.execute(deleteById.bind(symbol, date));
  }

  @NonNull
  public Optional<Stock> findById(@NonNull String symbol, @NonNull Instant date) {
    ResultSet rs = session.execute(findById.bind(symbol, date));
    return Optional.ofNullable(rs.one()).map(StockQueries::map);
  }

  @NonNull
  public QueryResults<Stock> findAllBySymbol(
      @NonNull String symbol,
      @NonNull Instant start,
      @NonNull Instant end,
      int pageSize,
      @Nullable ByteBuffer pagingState) {
    ResultSet rs =
        session.execute(
            StockQueries.bindRange(findBySymbol, symbol, start, end)
                .setPagingState(pagingState)
                .setPageSize(pageSize));
    Stream<Stock> results =
        StreamSupport.stream(rs.spliterator(), false)
            .limit(rs.getAvailableWithoutFetching())
            .map(StockQueries::map);
    ByteBuffer nextPage = rs.getExecutionInfo().getPagingState();
    return new QueryResults<>(results, nextPage);
  }
}
