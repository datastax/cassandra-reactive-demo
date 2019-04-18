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
package com.datastax.demo.sync.controller;

import com.datastax.demo.sync.dto.QueryResults;
import com.datastax.demo.sync.model.Stock;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Component
public class StockUriHelper {

  private final Converter<Instant, String> instantStringConverter;

  private final Converter<ByteBuffer, String> byteBufferStringConverter;

  public StockUriHelper(
      Converter<Instant, String> instantStringConverter,
      Converter<ByteBuffer, String> byteBufferStringConverter) {
    this.instantStringConverter = instantStringConverter;
    this.byteBufferStringConverter = byteBufferStringConverter;
  }

  public URI buildNextPageUri(QueryResults<Stock> results) {
    return results
        .getNextPage()
        .map(byteBufferStringConverter::convert)
        .map(
            encoded ->
                ServletUriComponentsBuilder.fromCurrentRequest()
                    .replaceQueryParam("page", encoded)
                    .build(true)
                    .toUri())
        .orElse(null);
  }

  public URI buildDetailsUri(Stock stock) {
    return ServletUriComponentsBuilder.fromCurrentRequestUri()
        .replacePath("/api/v1/stocks/{symbol}/{date}")
        .buildAndExpand(stock.getSymbol(), instantStringConverter.convert(stock.getDate()))
        .toUri();
  }
}
