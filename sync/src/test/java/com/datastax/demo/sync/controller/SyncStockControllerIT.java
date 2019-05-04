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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.demo.common.model.Stock;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * An integration test for the {@link SyncStockController}.
 *
 * <p>This test assumes that a DataStax Enterprise (DSE) or Apache Cassandra cluster is running and
 * accepting client connections.
 *
 * <p>The connection properties can be configured in the application.yml file.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles("integration-test")
@ComponentScan("com.datastax.demo")
class SyncStockControllerIT {

  private static final ParameterizedTypeReference<List<Stock>> LIST_OF_STOCKS =
      new ParameterizedTypeReference<>() {};

  @LocalServerPort private int port;

  @Autowired private TestRestTemplate template;

  @Autowired private DseSession session;

  @Autowired
  @Qualifier("stocks.prepared.insert")
  private PreparedStatement stockInsert;

  @Autowired
  @Qualifier("stocks.simple.truncate")
  private SimpleStatement stockTruncate;

  private Instant i1 = Instant.parse("2019-01-01T00:00:00Z");
  private Instant i2 = Instant.parse("2020-01-01T00:00:00Z");
  private Instant i3 = Instant.parse("2021-01-01T00:00:00Z");

  private Stock stock1 = new Stock("ABC", i1, BigDecimal.valueOf(42.0));
  private Stock stock2 = new Stock("ABC", i2, BigDecimal.valueOf(43.0));
  private Stock stock3 = new Stock("ABC", i3, BigDecimal.valueOf(44.0));

  private URI baseUri;
  private URI stock1Uri;
  private URI findStocksUri;

  @BeforeAll
  void prepareUris() {
    baseUri =
        UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("localhost")
            .port(port)
            .path("/api/v1/stocks")
            .build()
            .toUri();
    stock1Uri =
        UriComponentsBuilder.fromUri(baseUri).path("/ABC/20190101000000000").build().toUri();
    findStocksUri =
        UriComponentsBuilder.fromUri(baseUri)
            .path("/ABC")
            .queryParam("start", "2019")
            .queryParam("end", "2022")
            .queryParam("offset", "1")
            .queryParam("limit", "2")
            .build()
            .toUri();
  }

  @AfterEach
  void truncateTable() {
    session.execute(stockTruncate);
  }

  /** Tests that an existing stock value can be retrieved with a GET request to its specific URI. */
  @Test
  void should_find_stock_by_id() {
    // given
    insertStocks(stock1);
    // when
    Stock actual = template.getForObject(stock1Uri, Stock.class);
    // then
    assertThat(actual).isEqualTo(stock1);
  }

  /**
   * Tests that a non-existing stock value cannot be retrieved with a GET request to its specific
   * URI.
   */
  @Test
  void should_not_find_stock_by_id() {
    // when
    ResponseEntity<Stock> actual = template.getForEntity(stock1Uri, Stock.class);
    // then
    assertThat(actual.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(actual.hasBody()).isFalse();
  }

  /**
   * Tests that existing stock values for a given symbol and within a given date range can be
   * retrieved with a GET request to the appropriate URI.
   */
  @Test
  void should_find_stocks_by_symbol() {
    // given
    insertStocks(stock1, stock2, stock3);
    // when
    RequestEntity<Void> request1 = RequestEntity.get(findStocksUri).build();
    ResponseEntity<List<Stock>> response1 = template.exchange(request1, LIST_OF_STOCKS);
    // then
    assertThat(response1.getBody()).isEqualTo(List.of(stock2, stock1));
  }

  /** Tests that a stock value can be created via a POST request to the appropriate URI. */
  @Test
  void should_create_stock() {
    // when
    ResponseEntity<Stock> response = template.postForEntity(baseUri, stock1, Stock.class);
    // then
    assertThat(response.getHeaders().getFirst("Location")).isEqualTo(stock1Uri.toString());
    assertThat(response.getBody()).isEqualTo(stock1);
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .hasOnlyOneElementSatisfying(
            row -> {
              assertThat(row.getInstant(0)).isEqualTo(stock1.getDate());
              assertThat(row.getBigDecimal(1)).isEqualTo(stock1.getValue());
            });
  }

  /** Tests that an existing stock value can be updated via a PUT request to its specific URI. */
  @Test
  void should_update_stock() {
    // given
    insertStocks(stock1);
    BigDecimal newValue = BigDecimal.valueOf(42.42);
    Stock updated = new Stock(stock1.getSymbol(), stock1.getDate(), newValue);
    // when
    RequestEntity<Stock> request = RequestEntity.put(stock1Uri).body(updated);
    ResponseEntity<Stock> response = template.exchange(request, Stock.class);
    // then
    assertThat(response.getBody()).isEqualTo(updated);
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .hasOnlyOneElementSatisfying(
            row -> {
              assertThat(row.getInstant(0)).isEqualTo(updated.getDate());
              assertThat(row.getBigDecimal(1)).isEqualTo(updated.getValue());
            });
  }

  /**
   * Tests that a non-existing stock value cannot be updated via a PUT request to the appropriate
   * URI.
   */
  @Test
  void should_not_update_stock() {
    // given
    BigDecimal newValue = BigDecimal.valueOf(42.42);
    Stock updated = new Stock(stock1.getSymbol(), stock1.getDate(), newValue);
    // when
    RequestEntity<Stock> request = RequestEntity.put(stock1Uri).body(updated);
    ResponseEntity<Stock> response = template.exchange(request, Stock.class);
    // then
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(response.hasBody()).isFalse();
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .isEmpty();
  }

  /** Tests that an existing stock value can be deleted via a DELETE request to its specific URI. */
  @Test
  void should_delete_stock() {
    // given
    insertStocks(stock1);
    // when
    RequestEntity<Void> request = RequestEntity.delete(stock1Uri).build();
    ResponseEntity<Void> response = template.exchange(request, Void.class);
    // then
    assertThat(response.getBody()).isNull();
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .isEmpty();
  }

  private void insertStocks(Stock... stocks) {
    for (Stock stock : stocks) {
      BoundStatement bound = stockInsert.bind(stock.getSymbol(), stock.getDate(), stock.getValue());
      session.execute(bound);
    }
  }
}
