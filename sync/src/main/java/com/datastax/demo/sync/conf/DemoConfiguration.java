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
package com.datastax.demo.sync.conf;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

/** Connectivity to DSE (cassandra, graph, search). */
@Configuration
public class DemoConfiguration {

  /** Internal logger. */
  private static final Logger LOGGER = LoggerFactory.getLogger(DemoConfiguration.class);

  @Value("#{'${dse.contactPoints}'.split(',')}")
  private List<String> contactPoints;

  @Value("${dse.port: 9042}")
  private int port;

  @Value(
      "#{T(com.datastax.oss.driver.api.core.CqlIdentifier).fromInternal('${dse.keyspace: demo}')}")
  private CqlIdentifier keyspace;

  @Value("${dse.username}")
  private String dseUsername;

  @Value("${dse.password}")
  private String dsePassword;

  @Value("${dse.localdc: dc1}")
  private String localDc;

  @Bean
  public DseSession dseSession() {

    LOGGER.info("Initializing connection to DSE Cluster");
    LOGGER.info("Contact Points : {}", contactPoints);
    LOGGER.info("Listening Port : {}", port);
    LOGGER.info("Local DC : {}", localDc);
    LOGGER.info("Keyspace : {}", keyspace);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    DseSessionBuilder sessionBuilder = new DseSessionBuilder().withLocalDatacenter(localDc);

    contactPoints
        .stream()
        .map(cp -> InetSocketAddress.createUnresolved(cp, port))
        .forEach(sessionBuilder::addContactPoint);

    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "QUORUM")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));

    if (!StringUtils.isEmpty(dseUsername) && !StringUtils.isEmpty(dsePassword)) {
      LOGGER.info("Username : {}", dseUsername);
      configLoaderBuilder =
          configLoaderBuilder
              .withString(
                  DefaultDriverOption.AUTH_PROVIDER_CLASS, DsePlainTextAuthProvider.class.getName())
              .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, dseUsername)
              .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, dsePassword);
    }

    sessionBuilder = sessionBuilder.withConfigLoader(configLoaderBuilder.build());

    // First Connect without Keyspace (to create it if needed)
    try (DseSession tempSession = sessionBuilder.build()) {
      LOGGER.info("Creating keyspace {} (if needed)", keyspace);
      SimpleStatement createKeyspace =
          SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(1).build();
      tempSession.execute(createKeyspace);
    }

    // Now create the actual session
    DseSession dseSession = sessionBuilder.withKeyspace(keyspace).build();
    stopWatch.stop();
    LOGGER.info(
        "Connection established to DSE Cluster \\_0_/ in {} seconds.",
        stopWatch.getTotalTimeSeconds());
    return dseSession;
  }
}
