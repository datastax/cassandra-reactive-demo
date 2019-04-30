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

import static java.util.concurrent.CompletableFuture.completedStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import com.datastax.demo.common.model.Stock;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * A unit test for {@link AsyncStockRepository}.
 *
 * <p>This unit test does not require a Spring application context to be created.
 */
@ExtendWith(MockitoExtension.class)
class AsyncStockRepositoryTest {

  @Mock private DseSession session;

  @Mock private PreparedStatement insert;
  @Mock private PreparedStatement delete;
  @Mock private PreparedStatement findById;
  @Mock private PreparedStatement findBySymbol;

  @Mock private BoundStatement bound;

  @Mock private AsyncResultSet resultSet;

  @Mock private Row row1;
  @Mock private Row row2;

  @Mock private ExecutionInfo info;

  private Instant i1 = Instant.parse("2019-01-01T00:00:00Z");
  private Instant i2 = Instant.parse("2020-01-01T00:00:00Z");

  private Stock stock1 = new Stock("ABC", i1, BigDecimal.valueOf(42.0));
  private Stock stock2 = new Stock("ABC", i2, BigDecimal.valueOf(43.0));

  private ByteBuffer state1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private ByteBuffer state2 = ByteBuffer.wrap(new byte[] {4, 5, 6});

  @Test
  void should_save() {
    // given
    given(insert.bind(stock1.getSymbol(), stock1.getDate(), stock1.getValue())).willReturn(bound);
    given(session.executeAsync(bound)).willReturn(completedStage(resultSet));
    // when
    var stockRepository = new AsyncStockRepository(session, insert, delete, findById, findBySymbol);
    var future = stockRepository.save(stock1);
    // then
    assertThat(future).isNotNull().isCompletedWithValue(stock1);
    verify(session).executeAsync(bound);
  }

  @Test
  void should_delete_by_id() {
    // given
    given(delete.bind(stock1.getSymbol(), stock1.getDate())).willReturn(bound);
    given(session.executeAsync(bound)).willReturn(completedStage(resultSet));
    // when
    var stockRepository = new AsyncStockRepository(session, insert, delete, findById, findBySymbol);
    var future = stockRepository.deleteById(stock1.getSymbol(), stock1.getDate());
    // then
    assertThat(future).isNotNull().isCompletedWithValue(null);
    verify(session).executeAsync(bound);
  }

  @Test
  void should_find_by_id() throws ExecutionException, InterruptedException {
    // given
    given(findById.bind(stock1.getSymbol(), stock1.getDate())).willReturn(bound);
    given(session.executeAsync(bound)).willReturn(completedStage(resultSet));
    given(resultSet.one()).willReturn(row1);
    given(row1.getString(0)).willReturn(stock1.getSymbol());
    given(row1.getInstant(1)).willReturn(stock1.getDate());
    given(row1.getBigDecimal(2)).willReturn(stock1.getValue());
    // when
    var stockRepository = new AsyncStockRepository(session, insert, delete, findById, findBySymbol);
    var future = stockRepository.findById(stock1.getSymbol(), stock1.getDate());
    // then
    assertThat(future).isNotNull().isCompleted();
    assertThat(future.toCompletableFuture().get()).contains(stock1);
    verify(session).executeAsync(bound);
  }

  @Test
  void should_find_all_by_symbol() throws ExecutionException, InterruptedException {
    // given
    given(findBySymbol.bind("ABC", i1, i2)).willReturn(bound);
    given(bound.setPagingState(state1)).willReturn(bound);
    given(session.executeAsync(bound)).willReturn(completedStage(resultSet));
    List<Row> rows = List.of(row1, row2);
    given(resultSet.currentPage()).willReturn(rows);
    given(row1.getString(0)).willReturn(stock1.getSymbol());
    given(row1.getInstant(1)).willReturn(stock1.getDate());
    given(row1.getBigDecimal(2)).willReturn(stock1.getValue());
    given(row2.getString(0)).willReturn(stock2.getSymbol());
    given(row2.getInstant(1)).willReturn(stock2.getDate());
    given(row2.getBigDecimal(2)).willReturn(stock2.getValue());
    given(resultSet.getExecutionInfo()).willReturn(info);
    given(info.getPagingState()).willReturn(state2);
    // when
    var stockRepository = new AsyncStockRepository(session, insert, delete, findById, findBySymbol);
    var future = stockRepository.findAllBySymbol("ABC", i1, i2, state1);
    // then
    assertThat(future).isNotNull().isCompleted();
    var result = future.toCompletableFuture().get();
    assertThat(result.getResults()).containsExactly(stock1, stock2);
    assertThat(result.getNextPage()).isNotEmpty().contains(state2);
    verify(session).executeAsync(bound);
  }
}
