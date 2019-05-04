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
package com.datastax.demo.common.convert;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class InstantToStringConverterTest {

  @Test
  void should_convert_instant_to_string() {
    // given
    var input = Instant.parse("2019-01-01T12:34:56.789Z");
    var expected = "20190101123456789";
    // when
    InstantToStringConverter converter = new InstantToStringConverter();
    var actual = converter.convert(input);
    // then
    assertThat(actual).isEqualTo(expected);
  }
}
