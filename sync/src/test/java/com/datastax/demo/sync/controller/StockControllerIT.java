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

import java.net.URL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class StockControllerIT {

  @LocalServerPort private int port;

  private URL base;

  @Autowired private TestRestTemplate template;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
  }

  @Test
  void test() {
    ResponseEntity<String> response = template.getForEntity(base.toString(), String.class);
    assertThat(response.getBody()).isEqualTo("Greetings from Spring Boot!");
  }

  //  private static final long START_2010 = Instant.parse("2010-01-01T00:00:00Z").toEpochMilli();
  //  private static final long END_2020 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
  //
  //  @BeforeEach
  //  void setUp() {
  //
  //    CqlIdentifier keyspace = CqlIdentifier.fromCql("test" + System.nanoTime());
  //    session.execute(
  //        SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(1).build());
  //    session.execute(StockQueries.dropTable(keyspace));
  //    session.execute(StockQueries.createTable(keyspace));
  //    PreparedStatement insert = session.prepare(StockQueries.insert(keyspace));
  //    ThreadLocalRandom random = ThreadLocalRandom.current();
  //    for (int i = 0; i < 100; i++) {
  //      String symbol = "symbol" + i;
  //      for (int j = 0; j < 100; j++) {
  //        Instant date = Instant.ofEpochMilli(random.nextLong(START_2010, END_2020));
  //        double value = random.nextDouble(0d, 100d);
  //        session.execute(StockQueries.bind(insert, new Stock(symbol, date, value)));
  //      }
  //    }
  //  }
}
