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

import com.datastax.demo.common.convert.InstantToStringConverter;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.Instant;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.core.convert.converter.Converter;

/**
 * A serializer used to serialize {@link Instant} instances using a compact format: {@code
 * yyyyMMddHHmmssSSS}, always expressed in UTC.
 *
 * @see InstantToStringConverter
 */
@JsonComponent
public class InstantSerializer extends StdSerializer<Instant> {

  private final Converter<Instant, String> converter;

  public InstantSerializer(Converter<Instant, String> converter) {
    super(Instant.class);
    this.converter = converter;
  }

  @Override
  public void serialize(Instant instant, JsonGenerator gen, SerializerProvider serializerProvider)
      throws IOException {
    try {
      if (instant == null) {
        gen.writeNull();
      } else {
        gen.writeString(converter.convert(instant));
      }
    } catch (Exception e) {
      throw new JsonGenerationException("Could not serialize instant: " + instant, e, gen);
    }
  }
}
