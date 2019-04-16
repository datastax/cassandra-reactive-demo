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
package com.datastax.demo.sync.util;

import com.datastax.demo.sync.model.Stock;
import com.datastax.demo.sync.repository.StockQueries;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUpdater.class);

  private static final long START_2010 = Instant.parse("2010-01-01T00:00:00Z").toEpochMilli();
  private static final long END_2020 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();

  public static void updateSchema(CqlIdentifier keyspace, CqlSession session) {

    LOGGER.info("Updating schema");

    session.execute(
        SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(1).build());

    session.execute(StockQueries.dropTable(keyspace));
    session.execute(StockQueries.createTable(keyspace));

    PreparedStatement insert = session.prepare(StockQueries.insert(keyspace));

    ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < 100; i++) {

      String symbol = "symbol" + i;

      for (int j = 0; j < 100; j++) {
        Instant date = Instant.ofEpochMilli(random.nextLong(START_2010, END_2020));
        double value = random.nextDouble(0d, 100d);
        session.execute(StockQueries.bind(insert, new Stock(symbol, date, value)));
      }
    }

    LOGGER.info("Schema successfully updated");
  }
}
