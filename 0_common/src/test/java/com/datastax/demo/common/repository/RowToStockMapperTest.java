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
package com.datastax.demo.common.repository;

import static com.datastax.demo.common.conf.StockQueriesConfiguration.DATE;
import static com.datastax.demo.common.conf.StockQueriesConfiguration.SYMBOL;
import static com.datastax.demo.common.conf.StockQueriesConfiguration.VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.BDDMockito.given;

import com.datastax.demo.common.model.Stock;
import com.datastax.oss.driver.api.core.cql.Row;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RowToStockMapperTest {

  @Mock Row row;

  @Test
  void should_create_stock_when_invoked_given_valid_driver_row() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    given(row.getString(SYMBOL)).willReturn("ABC");
    given(row.getInstant(DATE)).willReturn(i);
    given(row.getBigDecimal(VALUE)).willReturn(value);
    // when
    RowToStockMapper mapper = new RowToStockMapper();
    Stock result = mapper.apply(row);
    // then
    assertThat(result.getSymbol()).isEqualTo("ABC");
    assertThat(result.getDate()).isEqualTo(i);
    assertThat(result.getValue()).isEqualTo(value);
  }

  @Test
  void should_error_out_when_invoked_given_invalid_driver_row() {
    {
      // given
      given(row.getString(SYMBOL)).willReturn(null);
      // when
      RowToStockMapper mapper = new RowToStockMapper();
      // then
      assertThatNullPointerException()
          .isThrownBy(() -> mapper.apply(row))
          .withMessage("column symbol cannot be null");
    }
    {
      // given
      given(row.getString(SYMBOL)).willReturn("ABC");
      given(row.getInstant(DATE)).willReturn(null);
      // when
      RowToStockMapper mapper = new RowToStockMapper();
      // then
      assertThatNullPointerException()
          .isThrownBy(() -> mapper.apply(row))
          .withMessage("column date cannot be null");
    }
    {
      // given
      given(row.getString(SYMBOL)).willReturn("ABC");
      given(row.getInstant(DATE)).willReturn(Instant.parse("2019-01-01T12:34:56.789Z"));
      given(row.getBigDecimal(VALUE)).willReturn(null);
      // when
      RowToStockMapper mapper = new RowToStockMapper();
      // then
      assertThatNullPointerException()
          .isThrownBy(() -> mapper.apply(row))
          .withMessage("column value cannot be null");
    }
  }
}
