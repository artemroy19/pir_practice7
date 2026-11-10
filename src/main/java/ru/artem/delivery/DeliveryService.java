package ru.artem.delivery;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Service
class DeliveryService {
    private final ConcurrentHashMap<Long, Entry> deliveries = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private final OrderClient orders;

    DeliveryService(OrderClient orders) {
        this.orders = orders;
    }

    Flux<Delivery> list(Delivery.Status status) {
        return Flux.defer(() -> Flux.fromIterable(deliveries.values())
                .map(entry -> entry.current)
                .filter(delivery -> status == null || delivery.status() == status)
                .sort(Comparator.comparingLong(Delivery::id)));
    }

    Mono<Delivery> get(long id) {
        return find(id).map(entry -> entry.current);
    }

    Mono<Delivery> create(Long orderId) {
        if (orderId == null || orderId <= 0) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Positive orderId required"));
        }
        return orders.find(orderId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")))
                .map(order -> {
                    Delivery delivery = new Delivery(ids.incrementAndGet(), order.id(), Delivery.Status.CREATED);
                    deliveries.put(delivery.id(), new Entry(delivery));
                    return delivery;
                });
    }

    Mono<Delivery> update(long id, Delivery.Status status) {
        if (status == null) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Status required"));
        }
        return find(id).map(entry -> entry.update(status));
    }

    Mono<Void> delete(long id) {
        return Mono.fromRunnable(() -> {
            Entry entry = deliveries.remove(id);
            if (entry == null) {
                throw missing(id);
            }
            entry.close();
        });
    }

    Flux<Delivery> stream(long id) {
        return find(id).flatMapMany(entry -> entry.events.asFlux());
    }

    private Mono<Entry> find(long id) {
        return Mono.defer(() -> Mono.justOrEmpty(deliveries.get(id)))
                .switchIfEmpty(Mono.error(missing(id)));
    }

    private static ResponseStatusException missing(long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Delivery " + id + " not found");
    }

    private static final class Entry {
        private final Sinks.Many<Delivery> events = Sinks.many().replay().latest();
        private volatile Delivery current;
        private boolean closed;

        Entry(Delivery delivery) {
            current = delivery;
            events.emitNext(delivery, Sinks.EmitFailureHandler.FAIL_FAST);
        }

        synchronized Delivery update(Delivery.Status status) {
            if (closed) {
                throw missing(current.id());
            }
            if (current.status() != status) {
                current = new Delivery(current.id(), current.orderId(), status);
                events.emitNext(current, Sinks.EmitFailureHandler.FAIL_FAST);
            }
            return current;
        }

        synchronized void close() {
            closed = true;
            events.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }
}
