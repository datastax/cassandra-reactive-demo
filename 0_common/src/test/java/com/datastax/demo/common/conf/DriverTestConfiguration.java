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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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
   * Creates a {@link ProgrammaticDriverConfigLoaderBuilder} bean suitable for integration tests.
   *
   * <p>In particular, this loader is configured to not emit warnings when issuing USE statements
   * (this is an anti-pattern in production, but acceptable in integration tests).
   *
   * <p>It is also configured to disable metadata completely (because no tests in this project need
   * them).
   *
   * <p>And finally, it is also configured to optimize some internal Netty values that speed up the
   * driver's shutdown sequence, thus speeding up tests.
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
        .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
        // Disable metadata entirely, we don't need them and disabling them speeds up tests
        .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
        // Set quiet period to zero to speed up tests
        .withInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0)
        .withInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0);
  }

  /**
   * Creates a session bean without binding it to an existing keyspace like we would normally do in
   * production.
   *
   * @param sessionBuilder The {@link CqlSessionBuilder} bean to use.
   * @param keyspace The {@linkplain CqlIdentifier keyspace} bean to use.
   * @return The {@link CqlSession} bean.
   */
  @Bean
  @Override
  public CqlSession session(
      @NonNull CqlSessionBuilder sessionBuilder,
      @Qualifier("keyspace") @NonNull CqlIdentifier keyspace) {
    // do not set the keyspace now since it needs to be created first, see createTestFixtures.
    return sessionBuilder.build();
  }

  /**
   * Creates a "slow" execution profile that can be used to perform DDL statements that usually take
   * longer to complete than regular statements.
   *
   * @param session The session ben to use.
   * @return A "slow" execution profile.
   */
  @Bean
  public DriverExecutionProfile slowProfile(CqlSession session) {
    return SessionUtils.slowProfile(session);
  }

  /**
   * Creates a test keyspace with a generated name and then creates a stocks table inside that
   * keyspace.
   */
  @PostConstruct
  public void createTestFixtures() {
    CqlIdentifier keyspace = keyspace();
    CqlSession session = session(sessionBuilder(configLoaderBuilder()), keyspace);
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
    CqlSession session = session(sessionBuilder(configLoaderBuilder()), keyspace);
    DriverExecutionProfile slowProfile = slowProfile(session);
    var dropKeyspace = SchemaBuilder.dropKeyspace(keyspace).ifExists().build();
    session.execute(dropKeyspace.setExecutionProfile(slowProfile));
  }
}
