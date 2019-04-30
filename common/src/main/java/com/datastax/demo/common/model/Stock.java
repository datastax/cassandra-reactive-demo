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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import org.springframework.lang.NonNull;

/** The value of a stock symbol at a point in time. */
public class Stock {

  private final String symbol;

  private final Instant date;

  private final BigDecimal value;

  @JsonCreator
  public Stock(
      @NonNull @JsonProperty("symbol") String symbol,
      @NonNull @JsonProperty("date") Instant date,
      @NonNull @JsonProperty("value") BigDecimal value) {
    this.symbol = symbol;
    this.date = date;
    this.value = value;
  }

  /** @return The stock symbol. */
  @NonNull
  public String getSymbol() {
    return symbol;
  }

  /** @return The date when the stock value was recorded. */
  @NonNull
  public Instant getDate() {
    return date;
  }

  /** @return The stock value. */
  @NonNull
  public BigDecimal getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Stock)) {
      return false;
    }
    Stock that = (Stock) o;
    return this.symbol.equals(that.symbol)
        && this.date.equals(that.date)
        && this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(symbol, date, value);
  }

  @Override
  public String toString() {
    return symbol + " at " + date + " = " + value;
  }
}
