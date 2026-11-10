package ru.artem.delivery;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@Component
class OrderClient {
    private final WebClient client;

    OrderClient(WebClient.Builder builder, @Value("${orders.base-url}") String baseUrl) {
        client = builder.baseUrl(baseUrl).build();
    }

    Mono<Order> find(long id) {
        return client.get().uri("/api/orders/{id}", id).retrieve()
                .bodyToMono(Order.class)
                .timeout(Duration.ofSeconds(3))
                .onErrorResume(WebClientResponseException.NotFound.class, error -> Mono.empty())
                .onErrorMap(TimeoutException.class, error ->
                        new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "Order Service timeout", error))
                .onErrorMap(WebClientException.class, error ->
                        new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Order Service unavailable", error));
    }

    record Order(long id) {}
}
