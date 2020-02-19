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
package com.datastax.demo.reactive.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.demo.common.model.Stock;
import com.datastax.oss.driver.api.core.CqlSession;
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
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * An integration test for the {@link ReactiveStockController}, using the modern {@link
 * WebTestClient} API.
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
class ReactiveStockControllerIT {

  @LocalServerPort private int port;

  @Autowired private WebTestClient webTestClient;

  @Autowired private CqlSession session;

  @Autowired
  @Qualifier("stocks.prepared.insert")
  private PreparedStatement stockInsert;

  @Autowired
  @Qualifier("stocks.simple.truncate")
  private SimpleStatement stockTruncate;

  private Instant i1 = Instant.parse("2019-01-01T00:00:00Z");
  private Instant i2 = Instant.parse("2020-01-01T00:00:00Z");
  private Instant i3 = Instant.parse("2021-01-01T00:00:00Z");
  private Instant i4 = Instant.parse("2022-01-01T00:00:00Z");
  private Instant i5 = Instant.parse("2023-01-01T00:00:00Z");

  private Stock stock1 = new Stock("ABC", i1, BigDecimal.valueOf(42.0));
  private Stock stock2 = new Stock("ABC", i2, BigDecimal.valueOf(43.0));
  private Stock stock3 = new Stock("ABC", i3, BigDecimal.valueOf(44.0));
  private Stock stock4 = new Stock("ABC", i4, BigDecimal.valueOf(45.0));
  private Stock stock5 = new Stock("ABC", i5, BigDecimal.valueOf(46.0));

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
            .queryParam("start", "2000")
            .queryParam("end", "2100")
            .queryParam("offset", "1")
            .queryParam("limit", "2")
            .build()
            .toUri();
  }

  @AfterEach
  void truncateTable() {
    session.execute(stockTruncate);
  }

  /** Tests that a stock value can be created via a POST request to the appropriate URI. */
  @Test
  void should_return_created_stock_when_POST_method_given_valid_request() {
    // when
    webTestClient
        .post()
        .uri(baseUri)
        .syncBody(stock1)
        .exchange()
        // then
        .expectStatus()
        .isCreated()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
        .expectHeader()
        .valueEquals("location", stock1Uri.toString())
        .expectBody(Stock.class)
        .isEqualTo(stock1);
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .hasOnlyOneElementSatisfying(
            row -> {
              assertThat(row.getInstant(0)).isEqualTo(stock1.getDate());
              assertThat(row.getBigDecimal(1)).isEqualTo(stock1.getValue());
            });
  }

  /** Tests that an existing stock value can be updated via a PUT request to its specific URI. */
  @Test
  void should_return_updated_stock_when_PUT_method_given_valid_request() {
    // given
    insertStocks(stock1);
    BigDecimal newValue = BigDecimal.valueOf(42.42);
    Stock updated = new Stock(stock1.getSymbol(), stock1.getDate(), newValue);
    // when
    webTestClient
        .put()
        .uri(stock1Uri)
        .syncBody(updated)
        .exchange()
        // then
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
        .expectBody(Stock.class)
        .isEqualTo(updated);
    assertThat(session.execute("SELECT date, value from stocks WHERE symbol = 'ABC'").all())
        .hasOnlyOneElementSatisfying(
            row -> {
              assertThat(row.getInstant(0)).isEqualTo(updated.getDate());
              assertThat(row.getBigDecimal(1)).isEqualTo(updated.getValue());
            });
  }

  /**
   * Tests that a non-existing stock value cannot be updated via PUT request to its specific URI and
   * results in an HTTP NotFound status.
   */
  @Test
  void should_return_not_found_when_PUT_method_given_invalid_request() {
    // given
    BigDecimal newValue = BigDecimal.valueOf(42.42);
    Stock updated = new Stock(stock1.getSymbol(), stock1.getDate(), newValue);
    // when
    webTestClient
        .put()
        .uri(stock1Uri)
        .syncBody(updated)
        .exchange()
        // then
        .expectStatus()
        .isNotFound()
        .expectBody()
        .isEmpty();
    assertThat(session.execute("SELECT * FROM stocks WHERE symbol = 'ABC'").all()).isEmpty();
  }

  /**
   * Tests that an existing stock value can be deleted via a DELETE request to its specific URI and
   * results in an HTTP OK status.
   */
  @Test
  void should_return_OK_when_DELETE_method_given_valid_request() {
    // given
    insertStocks(stock1);
    // when
    webTestClient
        .delete()
        .uri(stock1Uri)
        .exchange()
        // then
        .expectStatus()
        .isOk()
        .expectBody()
        .isEmpty();
    assertThat(session.execute("SELECT * FROM stocks WHERE symbol = 'ABC'").all()).isEmpty();
  }

  /** Tests that an existing stock value can be retrieved with a GET request to its specific URI. */
  @Test
  void should_return_found_stock_when_GET_method_given_valid_request() {
    // given
    insertStocks(stock1);
    // when
    webTestClient
        .get()
        .uri(stock1Uri)
        .exchange()
        // then
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
        .expectBody(Stock.class)
        .isEqualTo(stock1);
  }

  /**
   * Tests that a non-existing stock value cannot be retrieved with a GET request to its specific *
   * URI and results in an HTTP NotFound status.
   */
  @Test
  void should_return_not_found_when_GET_method_given_invalid_request() {
    // when
    webTestClient
        .get()
        .uri(stock1Uri)
        .exchange()
        // then
        .expectStatus()
        .isNotFound()
        .expectBody()
        .isEmpty();
  }

  /**
   * Tests that existing stock values for a given symbol and within a given date range can be
   * retrieved with a GET request to the appropriate URI.
   */
  @Test
  void should_return_found_stocks_when_GET_method_given_valid_request() {
    // given
    insertStocks(stock1, stock2, stock3, stock4, stock5);
    // when
    webTestClient
        .get()
        .uri(findStocksUri)
        .exchange()
        // then
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
        .expectBodyList(Stock.class)
        .isEqualTo(List.of(stock4, stock3));
  }

  private void insertStocks(Stock... stocks) {
    for (Stock stock : stocks) {
      BoundStatement bound = stockInsert.bind(stock.getSymbol(), stock.getDate(), stock.getValue());
      session.execute(bound);
    }
  }
}
