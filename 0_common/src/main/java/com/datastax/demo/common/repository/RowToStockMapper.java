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

import com.datastax.demo.common.model.Stock;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Objects;
import java.util.function.Function;
import org.springframework.stereotype.Component;

/**
 * A row mapping function that creates a {@link Stock} instance from a database {@link Row}.
 *
 * <p>The row is expected to contain all 3 columns in the <code>stocks</code> table: <code>symbol
 * </code>, <code>date</code> and <code>value</code>, as if it were obtained by a CQL query such as
 * {@code SELECT symbol, date, value FROM stocks WHERE ...}.
 */
@Component
public class RowToStockMapper implements Function<Row, Stock> {

  @Override
  public Stock apply(Row row) {
    var symbol = Objects.requireNonNull(row.getString(SYMBOL), "column symbol cannot be null");
    var date = Objects.requireNonNull(row.getInstant(DATE), "column date cannot be null");
    var value = Objects.requireNonNull(row.getBigDecimal(VALUE), "column value cannot be null");
    return new Stock(symbol, date, value);
  }
}
