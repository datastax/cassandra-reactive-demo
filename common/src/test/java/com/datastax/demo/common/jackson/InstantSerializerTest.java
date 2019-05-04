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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;

@ExtendWith(MockitoExtension.class)
class InstantSerializerTest {

  @Mock Converter<Instant, String> converter;

  @Mock JsonGenerator gen;

  @Mock SerializerProvider provider;

  @InjectMocks InstantSerializer serializer;

  @Test
  void should_serialize_instant_as_string() throws IOException {
    // given
    var expected = "20190101123456789";
    var input = Instant.parse("2019-01-01T12:34:56.789Z");
    given(converter.convert(input)).willReturn(expected);
    // when
    serializer.serialize(input, gen, provider);
    // then
    verify(converter).convert(input);
    verify(gen).writeString(expected);
  }

  @Test
  void should_serialize_null_as_null() throws IOException {
    // when
    serializer.serialize(null, gen, provider);
    // then
    verify(converter, never()).convert(any());
    verify(gen).writeNull();
  }

  @Test
  void should_throw_IOE_when_input_invalid() throws IOException {
    // given
    var error = new JsonGenerationException("not really", gen);
    var input = Instant.parse("2019-01-01T12:34:56.789Z");
    var converted = "20190101123456789";
    given(converter.convert(input)).willReturn(converted);
    willThrow(error).given(gen).writeString("20190101123456789");
    // when
    assertThatThrownBy(() -> serializer.serialize(input, gen, provider))
        // then
        .isInstanceOf(JsonGenerationException.class)
        .hasMessage("Could not serialize instant: " + input)
        .hasRootCause(error);
  }
}
