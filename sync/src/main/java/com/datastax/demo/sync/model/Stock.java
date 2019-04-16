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
package com.datastax.demo.sync.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;
import org.springframework.lang.NonNull;

public class Stock {

  private final String symbol;

  private final Instant date;

  private final double value;

  @JsonCreator
  public Stock(
      @NonNull @JsonProperty("symbol") String symbol,
      @NonNull @JsonProperty("date") Instant date,
      @JsonProperty("value") double value) {
    this.symbol = symbol;
    this.date = date;
    this.value = value;
  }

  @NonNull
  public String getSymbol() {
    return symbol;
  }

  @NonNull
  public Instant getDate() {
    return date;
  }

  public double getValue() {
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
    Stock stock = (Stock) o;
    return Double.compare(stock.getValue(), getValue()) == 0
        && getSymbol().equals(stock.getSymbol())
        && getDate().equals(stock.getDate());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSymbol(), getDate(), getValue());
  }

  @Override
  public String toString() {
    return symbol + " at " + date + " = " + value;
  }
}
