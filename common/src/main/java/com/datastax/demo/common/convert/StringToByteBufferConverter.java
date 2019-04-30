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

import java.nio.ByteBuffer;
import java.util.Base64;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * A {@link Converter} that converts Base 64 encoded Strings into {@link ByteBuffer} instances.
 *
 * <p>This converter is used to decode query parameters containing encoded paging states.
 */
@Component
public class StringToByteBufferConverter implements Converter<String, ByteBuffer> {

  @Override
  public ByteBuffer convert(@NonNull String source) {
    byte[] bytes = Base64.getUrlDecoder().decode(source);
    return ByteBuffer.wrap(bytes);
  }
}
