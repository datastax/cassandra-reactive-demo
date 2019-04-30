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

import com.datastax.demo.common.convert.StringToInstantConverter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import java.io.IOException;
import java.time.Instant;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.core.convert.converter.Converter;

/**
 * A deserializer used to deserialize {@link Instant} instances using a compact format: {@code
 * yyyyMMddHHmmssSSS}, always expressed in UTC.
 *
 * @see StringToInstantConverter
 */
@JsonComponent
public class InstantDeserializer extends StdScalarDeserializer<Instant> {

  private final Converter<String, Instant> converter;

  public InstantDeserializer(Converter<String, Instant> converter) {
    super(Instant.class);
    this.converter = converter;
  }

  @Override
  public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    try {
      String text = p.readValueAs(String.class);
      return text == null ? null : converter.convert(text);
    } catch (Exception e) {
      throw new JsonParseException(p, "Could not parse node as instant", e);
    }
  }
}
