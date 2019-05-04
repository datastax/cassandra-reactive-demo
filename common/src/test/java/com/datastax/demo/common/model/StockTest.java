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
package com.datastax.demo.common.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.cql.Row;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StockTest {

  @Mock Row row;

  @Test
  void should_create_stock_from_driver_row() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    given(row.getString(0)).willReturn("ABC");
    given(row.getInstant(1)).willReturn(i);
    given(row.getBigDecimal(2)).willReturn(value);
    // when
    Stock result = Stock.fromRow(row);
    // then
    assertThat(result.getSymbol()).isEqualTo("ABC");
    assertThat(result.getDate()).isEqualTo(i);
    assertThat(result.getValue()).isEqualTo(value);
  }

  @Test
  void should_return_true_when_two_instances_same() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i, value);
    // when
    @SuppressWarnings({"EqualsWithItself", "SelfEquals"})
    boolean actual = stock1.equals(stock1);
    // then
    assertThat(actual).isTrue();
  }

  @Test
  void should_return_true_when_two_instances_equal() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i, value);
    var stock2 = new Stock("ABC", i, value);
    // when
    boolean actual = stock1.equals(stock2);
    // then
    assertThat(actual).isTrue();
  }

  @Test
  void should_return_false_when_two_instances_have_different_symbols() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i, value);
    var stock2 = new Stock("DEF", i, value);
    // when
    boolean actual = stock1.equals(stock2);
    // then
    assertThat(actual).isFalse();
  }

  @Test
  void should_return_false_when_two_instances_have_different_dates() {
    // given
    var i1 = Instant.parse("2019-01-01T12:34:56.789Z");
    var i2 =
        LocalDateTime.of(2020, Month.MAY, 4, 10, 48, 47, 123_000_000).toInstant(ZoneOffset.UTC);
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i1, value);
    var stock2 = new Stock("ABC", i2, value);
    // when
    boolean actual = stock1.equals(stock2);
    // then
    assertThat(actual).isFalse();
  }

  @Test
  void should_return_false_when_two_instances_have_different_values() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value1 = BigDecimal.valueOf(42);
    BigDecimal value2 = BigDecimal.valueOf(43);
    var stock1 = new Stock("ABC", i, value1);
    var stock2 = new Stock("ABC", i, value2);
    // when
    boolean actual = stock1.equals(stock2);
    // then
    assertThat(actual).isFalse();
  }

  @Test
  void should_return_false_when_two_instances_not_same_class() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i, value);
    // when
    boolean actual = stock1.equals(new Object());
    // then
    assertThat(actual).isFalse();
  }

  @Test
  void should_return_same_hash_code_when_two_instances_equal() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    var stock1 = new Stock("ABC", i, value);
    var stock2 = new Stock("ABC", i, value);
    // when
    int hashCode1 = stock1.hashCode();
    int hashCode2 = stock2.hashCode();
    // then
    assertThat(hashCode1).isEqualTo(hashCode2);
  }

  @Test
  void should_return_formatted_stock_value() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    BigDecimal value = BigDecimal.valueOf(42);
    // when
    var stock = new Stock("ABC", i, value);
    var actual = stock.toString();
    // then
    assertThat(actual).isEqualTo("ABC at " + i + " = " + value);
  }
}
