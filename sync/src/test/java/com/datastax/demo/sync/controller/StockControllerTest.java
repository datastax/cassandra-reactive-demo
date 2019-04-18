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
package com.datastax.demo.sync.controller;

import static org.hamcrest.Matchers.endsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datastax.demo.sync.dto.QueryResults;
import com.datastax.demo.sync.model.Stock;
import com.datastax.demo.sync.repository.StockRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("unit-test")
class StockControllerTest {

  @Autowired private MockMvc mvc;

  @Autowired private StockRepository repository;

  private Instant i1 = Instant.parse("2019-01-01T00:00:00Z");
  private Instant i2 = Instant.parse("2020-01-01T00:00:00Z");
  private Instant i3 = Instant.parse("2021-01-01T00:00:00Z");

  private Stock stock1 = new Stock("ABC", i1, 42.0);
  private Stock stock2 = new Stock("ABC", i2, 43.0);
  private Stock stock3 = new Stock("ABC", i3, 44.0);

  @BeforeEach
  void setUp() {
    when(repository.findById("ABC", i1)).thenReturn(Optional.of(stock1));
    when(repository.findAllBySymbol("ABC", null, null, 10, null))
        .thenReturn(new QueryResults<>(List.of(stock1, stock2, stock3).stream(), null));
    when(repository.findAllBySymbol("ABC", i1, i3, 10, null))
        .thenReturn(new QueryResults<>(List.of(stock1, stock2).stream(), null));
  }

  @Test
  void should_find_stock_by_id() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/api/v1/stocks/ABC/20190101"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(
            content().json("{\"symbol\":\"ABC\",\"date\":\"20190101000000000\",\"value\":42.0}"));
  }

  @Test
  void should_find_stocks_by_symbol() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/api/v1/stocks/ABC/"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    "{\"results\":["
                        + "{\"value\":42.0,\"date\":\"20190101000000000\"},"
                        + "{\"value\":43.0,\"date\":\"20200101000000000\"},"
                        + "{\"value\":44.0,\"date\":\"20210101000000000\"}"
                        + "],\"nextPage\":null}"));
  }

  @Test
  void should_find_stocks_by_symbol_within_range() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/api/v1/stocks/ABC?start=2019&end=2021"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    "{\"results\":["
                        + "{\"value\":42.0,\"date\":\"20190101000000000\"},"
                        + "{\"value\":43.0,\"date\":\"20200101000000000\"}"
                        + "],\"nextPage\":null}"));
  }

  @Test
  void should_create_stock() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post("/api/v1/stocks/")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"symbol\":\"ABC\",\"date\":\"20220101\",\"value\":45.0}"))
        .andExpect(status().isCreated())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(header().string("Location", endsWith("/api/v1/stocks/ABC/20220101000000000")))
        .andExpect(
            content().json("{\"symbol\":\"ABC\",\"date\":\"20220101000000000\",\"value\":45.0}"));
    verify(repository).save(new Stock("ABC", Instant.parse("2022-01-01T00:00:00Z"), 45.0));
  }

  @Test
  void should_update_stock() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/api/v1/stocks/ABC/20190101")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"symbol\":\"ABC\",\"date\":\"20190101\",\"value\":41.0}"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(
            content().json("{\"symbol\":\"ABC\",\"date\":\"20190101000000000\",\"value\":41.0}"));
    verify(repository).save(new Stock("ABC", i1, 41.0));
  }

  @Test
  void should_delete_stock() throws Exception {
    mvc.perform(MockMvcRequestBuilders.delete("/api/v1/stocks/ABC/20190101"))
        .andExpect(status().isOk());
    verify(repository).deleteById("ABC", i1);
  }
}
