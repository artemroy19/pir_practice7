package ru.artem.delivery;

public record Delivery(long id, long orderId, Status status) {
    public enum Status {
        CREATED, ACCEPTED, IN_TRANSIT, DELIVERED
    }
}
