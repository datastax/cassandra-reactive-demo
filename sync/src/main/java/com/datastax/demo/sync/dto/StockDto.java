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
package com.datastax.demo.sync.dto;

import com.datastax.demo.sync.model.Stock;
import java.net.URI;
import java.time.Instant;
import org.springframework.lang.NonNull;

@SuppressWarnings("unused")
public class StockDto {

  private final Stock stock;

  private final URI details;

  public StockDto(@NonNull Stock stock, @NonNull URI details) {
    this.stock = stock;
    this.details = details;
  }

  @NonNull
  public Instant getDate() {
    return stock.getDate();
  }

  public double getValue() {
    return stock.getValue();
  }

  @NonNull
  public URI getDetails() {
    return details;
  }
}
