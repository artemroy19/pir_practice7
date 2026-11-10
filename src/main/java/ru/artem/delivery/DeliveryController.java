package ru.artem.delivery;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/deliveries")
class DeliveryController {
    private final DeliveryService service;

    DeliveryController(DeliveryService service) {
        this.service = service;
    }

    @GetMapping
    Flux<Delivery> list(@RequestParam(name = "status", required = false) Delivery.Status status) {
        return service.list(status);
    }

    @GetMapping("/{id}")
    Mono<Delivery> get(@PathVariable("id") long id) {
        return service.get(id);
    }

    @PostMapping
    Mono<ResponseEntity<Delivery>> create(@RequestBody Mono<CreateRequest> request) {
        return request.flatMap(body -> service.create(body.orderId()))
                .map(delivery -> ResponseEntity.created(URI.create("/api/deliveries/" + delivery.id()))
                        .body(delivery));
    }

    @PutMapping("/{id}")
    Mono<Delivery> update(@PathVariable("id") long id, @RequestBody Mono<UpdateRequest> request) {
        return request.flatMap(body -> service.update(id, body.status()));
    }

    @DeleteMapping("/{id}")
    Mono<ResponseEntity<Void>> delete(@PathVariable("id") long id) {
        return service.delete(id).thenReturn(ResponseEntity.noContent().build());
    }

    @GetMapping(value = "/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<Delivery>> stream(@PathVariable("id") long id) {
        return service.stream(id)
                .map(delivery -> ServerSentEvent.builder(delivery).event("delivery-status").build());
    }

    record CreateRequest(Long orderId) {}
    record UpdateRequest(Delivery.Status status) {}
}
