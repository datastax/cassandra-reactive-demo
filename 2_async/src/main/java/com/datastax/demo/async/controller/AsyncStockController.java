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
package com.datastax.demo.async.controller;

import com.datastax.demo.async.repository.AsyncStockRepository;
import com.datastax.demo.common.controller.StockUriHelper;
import com.datastax.demo.common.model.Stock;
import com.datastax.oss.driver.api.core.DriverException;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

/** A REST controller that performs CRUD actions on {@link Stock} instances. */
@RestController
@RequestMapping("/api/v1/stocks")
public class AsyncStockController {

  private final AsyncStockRepository stockRepository;

  private final StockUriHelper uriHelper;

  public AsyncStockController(AsyncStockRepository stockRepository, StockUriHelper uriHelper) {
    this.stockRepository = stockRepository;
    this.uriHelper = uriHelper;
  }

  /**
   * Converts {@link DriverException}s into HTTP 500 error codes and outputs the error message as
   * the response body.
   *
   * @param e The {@link DriverException}.
   * @return The error message to be used as response body.
   */
  @ExceptionHandler(Exception.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public String errorHandler(DriverException e) {
    return e.getMessage();
  }

  /**
   * Lists the available stocks for the given symbol and date range (GET method).
   *
   * @param symbol The symbol to list stocks for.
   * @param startInclusive The start of the date range (inclusive).
   * @param endExclusive The end of the date range (exclusive).
   * @param offset The zero-based index of the first result to return.
   * @param limit The maximum number of results to return.
   * @param request The current HTTP request.
   * @return The available stocks for the given symbol and date range.
   */
  @GetMapping("/{symbol}")
  public DeferredResult<ResponseEntity<Stream<Stock>>> listStocks(
      @PathVariable(name = "symbol") @NonNull String symbol,
      @RequestParam(name = "start") @NonNull Instant startInclusive,
      @RequestParam(name = "end") @NonNull Instant endExclusive,
      @RequestParam(name = "offset") int offset,
      @RequestParam(name = "limit") int limit,
      @NonNull HttpServletRequest request) {
    var deferred = new DeferredResult<ResponseEntity<Stream<Stock>>>();
    stockRepository
        .findAllBySymbol(symbol, startInclusive, endExclusive, offset, limit)
        .whenComplete(
            (results, error) -> {
              if (error == null) {
                deferred.setResult(ResponseEntity.ok(results));
              } else {
                deferred.setErrorResult(error);
              }
            });
    return deferred;
  }

  /**
   * Retrieves the stock value for the given symbol and date (GET method).
   *
   * @param symbol The stock symbol to find.
   * @param date The stock date to find.
   * @return The found stock value, or empty if no stock value was found.
   */
  @GetMapping("/{symbol}/{date}")
  public DeferredResult<ResponseEntity<Stock>> findStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    var deferred = new DeferredResult<ResponseEntity<Stock>>();
    stockRepository
        .findById(symbol, date)
        .whenComplete(
            (stock, error) -> {
              if (error == null) {
                var response =
                    stock.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
                deferred.setResult(response);
              } else {
                deferred.setErrorResult(error);
              }
            });
    return deferred;
  }

  /**
   * Creates a new stock value (POST method).
   *
   * @param stock The stock value to create.
   * @param request The current HTTP request.
   * @return The created stocked value.
   */
  @PostMapping("")
  public DeferredResult<ResponseEntity<Stock>> createStock(
      @RequestBody Stock stock, @NonNull HttpServletRequest request) {
    var deferred = new DeferredResult<ResponseEntity<Stock>>();
    stockRepository
        .save(stock)
        .whenComplete(
            (created, error) -> {
              if (error == null) {
                URI location = uriHelper.buildDetailsUri(request, created);
                deferred.setResult(ResponseEntity.created(location).body(created));
              } else {
                deferred.setErrorResult(error);
              }
            });
    return deferred;
  }

  /**
   * Updates the stock value at the given path (PUT method).
   *
   * @param symbol The stock symbol to update.
   * @param date The stock date to update.
   * @param stock The new stock value.
   * @return The updated stock value.
   */
  @PutMapping("/{symbol}/{date}")
  public DeferredResult<ResponseEntity<Stock>> updateStock(
      @PathVariable("symbol") String symbol,
      @PathVariable("date") Instant date,
      @RequestBody Stock stock) {
    var deferred = new DeferredResult<ResponseEntity<Stock>>();
    stockRepository
        .findById(symbol, date)
        .whenComplete(
            (maybeFound, error1) -> {
              if (error1 == null) {
                maybeFound
                    .map(found -> new Stock(found.getSymbol(), found.getDate(), stock.getValue()))
                    .ifPresentOrElse(
                        toUpdate ->
                            stockRepository
                                .save(toUpdate)
                                .whenComplete(
                                    (found, error2) -> {
                                      if (error2 == null) {
                                        deferred.setResult(ResponseEntity.ok(found));
                                      } else {
                                        deferred.setErrorResult(error2);
                                      }
                                    }),
                        () -> deferred.setResult(ResponseEntity.notFound().build()));
              } else {
                deferred.setErrorResult(error1);
              }
            });
    return deferred;
  }

  /**
   * Deletes a stock value (DELETE method).
   *
   * @param symbol The stock symbol to delete.
   * @param date The stock date to delete.
   * @return An empty response.
   */
  @DeleteMapping("/{symbol}/{date}")
  public DeferredResult<ResponseEntity<Void>> deleteStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    var deferred = new DeferredResult<ResponseEntity<Void>>();
    stockRepository
        .deleteById(symbol, date)
        .whenComplete(
            (res, error) -> {
              if (error == null) {
                deferred.setResult(ResponseEntity.ok().build());
              } else {
                deferred.setErrorResult(error);
              }
            });
    return deferred;
  }
}
