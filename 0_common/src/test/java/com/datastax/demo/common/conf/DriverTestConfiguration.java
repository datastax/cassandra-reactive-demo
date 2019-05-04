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
package com.datastax.demo.common.conf;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;

/**
 * A special configuration class that overrides some of the bean definitions declared in {@link
 * DriverConfiguration} with ones suitable for integration tests.
 *
 * <p>In particular, this configuration class generates a new keyspace name for each integration
 * test, then creates and destroys a test keyspace for each one of them.
 */
@Profile("integration-test")
@Configuration
public class DriverTestConfiguration extends DriverConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DriverTestConfiguration.class);

  @Autowired
  @Qualifier("stocks.simple.create")
  private SimpleStatement stockCreateTable;

  /**
   * Generates a new keyspace name for each test.
   *
   * @return The {@linkplain CqlIdentifier keyspace} bean.
   */
  @Override
  public CqlIdentifier keyspace() {
    return SessionUtils.uniqueKeyspaceId();
  }

  /**
   * Creates a {@link ProgrammaticDriverConfigLoaderBuilder} bean that does not emit warnings when
   * issuing USE statements (this is an anti-pattern in production, but acceptable in integration
   * tests).
   *
   * @return The {@link ProgrammaticDriverConfigLoaderBuilder} bean.
   */
  @Override
  public ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return super.configLoaderBuilder()
        // Do not warn on keyspace changes since we are going to set the keyspace after the session
        // is created
        .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
        // Artificially set the page size to a ridiculously low value to make sure integration tests
        // exercise pagination
        .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2);
  }

  /**
   * Creates a session bean without binding it to an existing keyspace like we would normally do in
   * production.
   *
   * @param sessionBuilder The {@link DseSessionBuilder} bean to use.
   * @param driverConfigLoaderBuilder The {@link ProgrammaticDriverConfigLoaderBuilder} bean to use.
   * @param keyspace The {@linkplain CqlIdentifier keyspace} bean to use.
   * @return The {@link DseSession} bean.
   */
  @Bean
  @Override
  public DseSession session(
      @NonNull DseSessionBuilder sessionBuilder,
      @NonNull ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder,
      @Qualifier("keyspace") @NonNull CqlIdentifier keyspace) {
    try {
      return sessionBuilder.withConfigLoader(driverConfigLoaderBuilder.build()).build();
    } catch (AllNodesFailedException e) {
      LOGGER.error(
          String.format(
              "Could not reach any contact point, "
                  + "make sure you've provided valid addresses (%s) and port (%d).",
              contactPoints, port));
      throw e;
    }
  }

  /**
   * Creates a "slow" execution profile that can be used to perform DDL statements that usually take
   * longer to complete than regular statements.
   *
   * @param session The session ben to use.
   * @return A "slow" execution profile.
   */
  @Bean
  public DriverExecutionProfile slowProfile(DseSession session) {
    return SessionUtils.slowProfile(session);
  }

  /**
   * Creates a test keyspace with a generated name and then creates a stocks table inside that
   * keyspace.
   */
  @PostConstruct
  public void createTestFixtures() {
    CqlIdentifier keyspace = keyspace();
    DseSession session = session(sessionBuilder(), configLoaderBuilder(), keyspace);
    DriverExecutionProfile slowProfile = slowProfile(session);
    var createKeyspace =
        SchemaBuilder.createKeyspace(keyspace)
            .ifNotExists()
            .withSimpleStrategy(1)
            .withDurableWrites(false)
            .build();
    session.execute(createKeyspace.setExecutionProfile(slowProfile));
    session.execute("USE " + keyspace);
    session.execute(stockCreateTable.setExecutionProfile(slowProfile));
  }

  /** Destroys the test keyspace completely. */
  @PreDestroy
  public void destroyTestFixtures() {
    CqlIdentifier keyspace = keyspace();
    DseSession session = session(sessionBuilder(), configLoaderBuilder(), keyspace);
    DriverExecutionProfile slowProfile = slowProfile(session);
    var dropKeyspace = SchemaBuilder.dropKeyspace(keyspace).ifExists().build();
    session.execute(dropKeyspace.setExecutionProfile(slowProfile));
  }
}
