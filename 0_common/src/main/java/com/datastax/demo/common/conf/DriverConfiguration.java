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
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader;
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;
import org.springframework.util.StringUtils;

/**
 * The DSE (DataStax Enterprise) Driver configuration.
 *
 * <p>Driver options should be specified in the usual way, that is, through an <code>
 * application.conf</code> file accessible on the application classpath. See the <a
 * href="https://docs.datastax.com/en/developer/java-driver-dse/2.0/manual/core/configuration/">driver
 * configuration</a> section in the online docs for more information.
 *
 * <p>To illustrate how to integrate the driver configuration with Spring, a few driver options
 * should be configured through Spring's own configuration mechanism:
 *
 * <ol>
 *   <li><code>driver.contactPoints</code>: this property will override the driver's {@code
 *       datastax-java-driver.basic.contact-points} option; it will default to <code>127.0.0.1
 *       </code> if unspecified;
 *   <li><code>driver.port</code>: this property will be combined with the previous one to create
 *       initial contact points; it will default to <code>9042</code> if unspecified;
 *   <li><code>driver.localdc</code>: this property will override the driver's {@code
 *       datastax-java-driver.basic.load-balancing-policy.local-datacenter} option; it has no
 *       default value and must be specified;
 *   <li><code>driver.keyspace</code>: this property will override the driver's {@code
 *       datastax-java-driver.basic.session-keyspace} option; it has no default value and must be
 *       specified;
 *   <li><code>driver.consistency</code>: this property will override the driver's {@code
 *       datastax-java-driver.basic.request.consistency}; it will default to <code>LOCAL_QUORUM
 *       </code> if unspecified;
 *   <li><code>driver.pageSize</code>: this property will override the driver's {@code
 *       datastax-java-driver.basic.request.page-size}; it will default to <code>10</code> if
 *       unspecified;
 *   <li><code>driver.username</code>: this property will override the driver's {@code
 *       datastax-java-driver.advanced.auth-provider.username} option; if unspecified, it will be
 *       assumed that no authentication is required;
 *   <li><code>driver.password</code>: this property will override the driver's {@code
 *       datastax-java-driver.advanced.auth-provider.password} option; if unspecified, it will be
 *       assumed that no authentication is required;
 * </ol>
 *
 * The above properties should be typically declared in an {@code application.yml} file.
 */
@Configuration
@Profile("!unit-test & !integration-test")
public class DriverConfiguration {

  @Value("#{'${driver.contactPoints}'.split(',')}")
  protected List<String> contactPoints;

  @Value("${driver.port:9042}")
  protected int port;

  @Value("${driver.localdc}")
  protected String localDc;

  @Value("${driver.keyspace}")
  protected String keyspaceName;

  @Value("${driver.consistency:LOCAL_QUORUM}")
  protected String consistency;

  @Value("${driver.username}")
  protected String dseUsername;

  @Value("${driver.password}")
  protected String dsePassword;

  /**
   * Returns the keyspace to connect to. The keyspace specified here must exist.
   *
   * @return The {@linkplain CqlIdentifier keyspace} bean.
   */
  @Bean
  public CqlIdentifier keyspace() {
    return CqlIdentifier.fromCql(keyspaceName);
  }

  /**
   * Returns a {@link ProgrammaticDriverConfigLoaderBuilder} to load driver options. This loader
   * will override values found in application.conf with properties found in application.yml.
   *
   * @return The {@link ProgrammaticDriverConfigLoaderBuilder} bean.
   */
  @Bean
  public ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
        DseDriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistency);
    if (!StringUtils.isEmpty(dseUsername) && !StringUtils.isEmpty(dsePassword)) {
      configLoaderBuilder =
          configLoaderBuilder
              .withString(
                  DefaultDriverOption.AUTH_PROVIDER_CLASS, DsePlainTextAuthProvider.class.getName())
              .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, dseUsername)
              .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, dsePassword);
    }
    return configLoaderBuilder;
  }

  /**
   * Returns a {@link DseSessionBuilder} that will configure sessions using contact points and other
   * configuration properties found in application.yml, merged with other options found in
   * application.conf.
   *
   * @return The {@link DseSessionBuilder} bean.
   */
  @Bean
  public DseSessionBuilder sessionBuilder() {
    DseSessionBuilder sessionBuilder = new DseSessionBuilder();
    for (String contactPoint : contactPoints) {
      InetSocketAddress address = InetSocketAddress.createUnresolved(contactPoint, port);
      sessionBuilder = sessionBuilder.addContactPoint(address);
    }
    return sessionBuilder.withLocalDatacenter(localDc);
  }

  /**
   * Returns the {@link DseSession} to use, configured with the provided {@link DseSessionBuilder
   * session builder} and {@link ProgrammaticDriverConfigLoaderBuilder config loader builder}. The
   * returned session will be automatically connected to the given keyspace.
   *
   * @param sessionBuilder The {@link DseSessionBuilder} bean to use.
   * @param driverConfigLoaderBuilder The {@link ProgrammaticDriverConfigLoaderBuilder} bean to use.
   * @param keyspace The {@linkplain CqlIdentifier keyspace} bean to use.
   * @return The {@link DseSession} bean.
   */
  @Bean
  public DseSession session(
      @NonNull DseSessionBuilder sessionBuilder,
      @NonNull ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder,
      @Qualifier("keyspace") @NonNull CqlIdentifier keyspace) {
    return sessionBuilder
        .withConfigLoader(driverConfigLoaderBuilder.build())
        .withKeyspace(keyspace)
        .build();
  }
}
