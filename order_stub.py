import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class OrderHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        orders = {"/api/orders/101": 101, "/api/orders/102": 102}
        order_id = orders.get(self.path)
        status = 200 if order_id is not None else 404
        data = {"id": order_id} if status == 200 else {"message": "Order not found"}
        body = json.dumps(data).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print("Order Service: http://127.0.0.1:8081", flush=True)
    with ThreadingHTTPServer(("127.0.0.1", 8081), OrderHandler) as server:
        server.serve_forever()
