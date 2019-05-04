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
package com.datastax.demo.common.controller;

import com.datastax.demo.common.model.Stock;
import java.net.URI;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/** A helper class that creates URIs for controllers dealing with {@link Stock} objects. */
@Component
public class StockUriHelper {

  private final Converter<Instant, String> instantStringConverter;

  public StockUriHelper(Converter<Instant, String> instantStringConverter) {
    this.instantStringConverter = instantStringConverter;
  }

  /**
   * Creates an URI pointing to a specific stock value.
   *
   * @param request The HTTP request that will serve as the base for new URI.
   * @param stock The stock value to create an URI for.
   * @return An URI pointing to a specific stock value.
   */
  @NonNull
  public URI buildDetailsUri(@NonNull HttpServletRequest request, @NonNull Stock stock) {
    String date = instantStringConverter.convert(stock.getDate());
    return ServletUriComponentsBuilder.fromRequestUri(request)
        .replacePath("/api/v1/stocks/{symbol}/{date}")
        .buildAndExpand(stock.getSymbol(), date)
        .toUri();
  }
}
