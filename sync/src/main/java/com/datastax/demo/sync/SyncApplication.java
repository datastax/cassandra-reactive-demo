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
package com.datastax.demo.sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Starts the application.
 *
 * <p>This application assumes that a DataStax Enterprise (DSE) or Apache Cassandra cluster is
 * running and accepting client connections.
 *
 * <p>The connection properties can be configured in the application.yml file.
 *
 * <p>The application assumes that a keyspace {@code demo} and a table {@code demo.stocks} both
 * exist with the following schema:
 *
 * <pre>{@code
 * CREATE KEYSPACE IF NOT EXISTS demo
 *   WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
 *   AND durable_writes = false;
 * CREATE TABLE IF NOT EXISTS demo.stocks
 *   (symbol text, date timestamp, value decimal, PRIMARY KEY (symbol, date))
 *   WITH CLUSTERING ORDER BY (date DESC);
 * }</pre>
 */
@SpringBootApplication(scanBasePackages = "com.datastax.demo")
public class SyncApplication {

  public static void main(String[] args) {
    SpringApplication.run(SyncApplication.class, args);
  }
}
