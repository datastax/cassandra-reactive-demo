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
package com.datastax.demo.common.jackson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;

@ExtendWith(MockitoExtension.class)
class InstantDeserializerTest {

  @Mock Converter<String, Instant> converter;

  @Mock JsonParser parser;

  @Mock DeserializationContext ctx;

  @InjectMocks InstantDeserializer deserializer;

  @Test
  void should_deserialize_string_as_instant() throws IOException {
    // given
    var input = "20190101123456789";
    var expected = Instant.parse("2019-01-01T12:34:56.789Z");
    given(parser.readValueAs(String.class)).willReturn(input);
    given(converter.convert(input)).willReturn(expected);
    // when
    var actual = deserializer.deserialize(parser, ctx);
    // then
    assertThat(actual).isEqualTo(expected);
    verify(parser).readValueAs(String.class);
    verify(converter).convert(input);
  }

  @Test
  void should_deserialize_null_as_null() throws IOException {
    // given
    given(parser.readValueAs(String.class)).willReturn(null);
    // when
    var actual = deserializer.deserialize(parser, ctx);
    // then
    assertThat(actual).isNull();
    verify(parser).readValueAs(String.class);
    verify(converter, never()).convert(any());
  }

  @Test
  void should_throw_IOE_when_input_invalid() throws IOException {
    // given
    var error = new JsonParseException(parser, "not really");
    given(parser.readValueAs(String.class)).willThrow(error);
    // when
    assertThatThrownBy(() -> deserializer.deserialize(parser, ctx))
        // then
        .isInstanceOf(JsonParseException.class)
        .hasMessage("Could not parse node as instant")
        .hasRootCause(error);
    verify(parser).readValueAs(String.class);
  }
}
