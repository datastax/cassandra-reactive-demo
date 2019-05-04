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
package com.datastax.demo.common.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.demo.common.model.Stock;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;

@ExtendWith(MockitoExtension.class)
class StockUriHelperTest {

  @Mock Converter<Instant, String> converter;

  @InjectMocks StockUriHelper stockUriHelper;

  @Mock HttpServletRequest request;

  @Test
  void should_build_details_uri_from_request_and_stock_value() {
    // given
    var i = Instant.parse("2019-01-01T12:34:56.789Z");
    var input = new Stock("ABC", i, BigDecimal.valueOf(42));
    given(converter.convert(i)).willReturn("20190101123456789");
    given(request.getScheme()).willReturn("http");
    given(request.getServerName()).willReturn("localhost");
    given(request.getServerPort()).willReturn(8080);
    // when
    URI actual = stockUriHelper.buildDetailsUri(request, input);
    // then
    assertThat(actual)
        .hasScheme("http")
        .hasPort(8080)
        .hasPath("/api/v1/stocks/ABC/20190101123456789");
  }
}
