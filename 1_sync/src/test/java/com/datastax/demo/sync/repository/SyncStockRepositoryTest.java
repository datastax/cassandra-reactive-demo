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

import static com.datastax.demo.common.conf.StockQueriesConfiguration.DATE;
import static com.datastax.demo.common.conf.StockQueriesConfiguration.SYMBOL;
import static com.datastax.demo.common.conf.StockQueriesConfiguration.VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import com.datastax.demo.common.model.Stock;
import com.datastax.demo.common.repository.RowToStockMapper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * A unit test for {@link SyncStockRepository}.
 *
 * <p>This unit test does not require a Spring application context to be created.
 */
@ExtendWith(MockitoExtension.class)
class SyncStockRepositoryTest {

  @Mock private CqlSession session;

  @Mock private PreparedStatement insert;
  @Mock private PreparedStatement delete;
  @Mock private PreparedStatement findById;
  @Mock private PreparedStatement findBySymbol;

  @Mock private BoundStatement bound;

  @Mock private ResultSet resultSet;

  @Mock private Row row1;
  @Mock private Row row2;

  private Instant i1 = Instant.parse("2019-01-01T00:00:00Z");
  private Instant i2 = Instant.parse("2020-01-01T00:00:00Z");

  private Stock stock1 = new Stock("ABC", i1, BigDecimal.valueOf(42.0));
  private Stock stock2 = new Stock("ABC", i2, BigDecimal.valueOf(43.0));

  private Function<Row, Stock> rowMapper = new RowToStockMapper();

  @Test
  void should_save_stock_when_save_given_valid_input() {
    // given
    given(insert.bind(stock1.getSymbol(), stock1.getDate(), stock1.getValue())).willReturn(bound);
    // when
    var stockRepository =
        new SyncStockRepository(session, insert, delete, findById, findBySymbol, rowMapper);
    var saved = stockRepository.save(stock1);
    // then
    assertThat(saved).isNotNull().isEqualTo(stock1);
    verify(session).execute(bound);
  }

  @Test
  void should_delete_stock_when_delete_by_id_given_valid_input() {
    // given
    given(delete.bind(stock1.getSymbol(), stock1.getDate())).willReturn(bound);
    // when
    var stockRepository =
        new SyncStockRepository(session, insert, delete, findById, findBySymbol, rowMapper);
    stockRepository.deleteById(stock1.getSymbol(), stock1.getDate());
    // then
    verify(session).execute(bound);
  }

  @Test
  void should_find_stock_when_find_by_id_given_valid_input() {
    // given
    given(findById.bind(stock1.getSymbol(), stock1.getDate())).willReturn(bound);
    given(session.execute(bound)).willReturn(resultSet);
    given(resultSet.one()).willReturn(row1);
    given(row1.getString(SYMBOL)).willReturn(stock1.getSymbol());
    given(row1.getInstant(DATE)).willReturn(stock1.getDate());
    given(row1.getBigDecimal(VALUE)).willReturn(stock1.getValue());
    // when
    var stockRepository =
        new SyncStockRepository(session, insert, delete, findById, findBySymbol, rowMapper);
    Optional<Stock> result = stockRepository.findById(stock1.getSymbol(), stock1.getDate());
    // then
    assertThat(result).isNotEmpty().contains(stock1);
    verify(session).execute(bound);
  }

  @Test
  void should_find_stocks_when_find_all_by_symbol_given_valid_input() {
    // given
    given(findBySymbol.bind("ABC", i1, i2)).willReturn(bound);
    given(session.execute(bound)).willReturn(resultSet);
    given(resultSet.one()).willReturn(row1, row2, null);
    given(row1.getString(SYMBOL)).willReturn(stock1.getSymbol());
    given(row1.getInstant(DATE)).willReturn(stock1.getDate());
    given(row1.getBigDecimal(VALUE)).willReturn(stock1.getValue());
    given(row2.getString(SYMBOL)).willReturn(stock2.getSymbol());
    given(row2.getInstant(DATE)).willReturn(stock2.getDate());
    given(row2.getBigDecimal(VALUE)).willReturn(stock2.getValue());
    // when
    var stockRepository =
        new SyncStockRepository(session, insert, delete, findById, findBySymbol, rowMapper);
    var result = stockRepository.findAllBySymbol("ABC", i1, i2, 0, 10);
    // then
    assertThat(result).isNotEmpty().containsExactly(stock1, stock2);
    verify(session).execute(bound);
  }
}
