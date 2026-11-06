import json
import socket
import time
import random
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from todo.model.todo import Todo

APP_NAME = "todo-app-python"
HOST = "0.0.0.0"
PORT = 8000
LOGSTASH_HOST = "localhost"
LOGSTASH_PORT = 5001

# Métricas Prometheus con etiqueta application="todo-app-python"
HTTP_REQUESTS_TOTAL = Counter(
    "todo_http_requests_total",
    "Total de peticiones HTTP recibidas en todo-app-python",
    ["application", "endpoint", "method", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "todo_http_request_duration_seconds",
    "Duración de las peticiones HTTP en segundos",
    ["application", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
TASKS_PENDING_GAUGE = Gauge(
    "todo_tasks_pending_total",
    "Número actual de tareas pendientes en TodoBook",
    ["application"],
)
TASKS_COMPLETED_COUNTER = Counter(
    "todo_tasks_completed_total",
    "Total acumulado de tareas completadas en TodoBook",
    ["application"],
)
APP_ERRORS_TOTAL = Counter(
    "todo_app_errors_total",
    "Total de errores de negocio registrados en todo-app-python",
    ["application", "error_type"],
)

# Estado en memoria usando el modelo Todo del repositorio ToDoApp
TODOS: dict[int, Todo] = {
    1: Todo(1, "Configurar Prometheus", "Registrar job MetricasTodoPython en prometheus.yml"),
    2: Todo(2, "Construir Dashboard Grafana", "Crear paneles PromQL para todo-app-python"),
    3: Todo(3, "Integrar Logstash TCP", "Enviar logs JSON al puerto 5001 con campo application"),
}
TASKS_PENDING_GAUGE.labels(application=APP_NAME).set(len(TODOS))


def send_logstash_event(level: str, message: str, endpoint: str, extra: dict | None = None):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    payload = {
        "@timestamp": now_iso,
        "level": level,
        "message": message,
        "logger_name": "todo.server_monitoreo",
        "thread_name": threading.current_thread().name,
        "application": APP_NAME,
        "endpoint": endpoint,
    }
    if extra:
        payload.update(extra)
    line = json.dumps(payload, ensure_ascii=False) + "\n"
    print(f"[{now_iso}] {level:5s} [{APP_NAME}] ({endpoint}) - {message}", flush=True)
    try:
        with socket.create_connection((LOGSTASH_HOST, LOGSTASH_PORT), timeout=1.5) as sock:
            sock.sendall(line.encode("utf-8"))
    except Exception as exc:
        print(f"[WARN] No se pudo enviar log a Logstash ({LOGSTASH_HOST}:{LOGSTASH_PORT}): {exc}", flush=True)


class TodoMonitoringHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return  # Usamos nuestro logger estructurado hacia consola y Logstash

    def do_GET(self):
        start_t = time.perf_counter()
        path = self.path.split("?")[0]

        if path == "/metrics":
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path in ("/", "/api/tareas"):
            new_id = len(TODOS) + 1
            t_obj = Todo(new_id, f"Tarea #{new_id}", "Generada desde endpoint /api/tareas")
            TODOS[new_id] = t_obj
            pending = sum(1 for t in TODOS.values() if not t.completed)
            TASKS_PENDING_GAUGE.labels(application=APP_NAME).set(pending)
            time.sleep(random.uniform(0.008, 0.045))
            dur = time.perf_counter() - start_t
            HTTP_REQUESTS_TOTAL.labels(application=APP_NAME, endpoint="/api/tareas", method="GET", status="200").inc()
            HTTP_REQUEST_DURATION.labels(application=APP_NAME, endpoint="/api/tareas").observe(dur)
            send_logstash_event(
                "INFO",
                f"Consulta y registro exitoso en TodoBook: {t_obj} (pendientes={pending})",
                "/api/tareas",
                {"todo_id": new_id, "pending_count": pending},
            )
            body = json.dumps(
                {
                    "application": APP_NAME,
                    "status": "OK",
                    "mensaje": f"Tarea creada y listada correctamente ({t_obj})",
                    "total_tareas": len(TODOS),
                    "pendientes": pending,
                },
                ensure_ascii=False,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/completar":
            pending_list = [t for t in TODOS.values() if not t.completed]
            if pending_list:
                pending_list[0].mark_completed()
                TASKS_COMPLETED_COUNTER.labels(application=APP_NAME).inc()
            pending = sum(1 for t in TODOS.values() if not t.completed)
            TASKS_PENDING_GAUGE.labels(application=APP_NAME).set(pending)
            dur = time.perf_counter() - start_t
            HTTP_REQUESTS_TOTAL.labels(application=APP_NAME, endpoint="/api/completar", method="GET", status="200").inc()
            HTTP_REQUEST_DURATION.labels(application=APP_NAME, endpoint="/api/completar").observe(dur)
            send_logstash_event(
                "INFO",
                f"Tarea marcada como completada en TodoBook (pendientes restantes={pending})",
                "/api/completar",
            )
            body = json.dumps({"application": APP_NAME, "status": "COMPLETED", "pendientes": pending}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/error":
            time.sleep(random.uniform(0.015, 0.08))
            dur = time.perf_counter() - start_t
            HTTP_REQUESTS_TOTAL.labels(application=APP_NAME, endpoint="/api/error", method="GET", status="500").inc()
            HTTP_REQUEST_DURATION.labels(application=APP_NAME, endpoint="/api/error").observe(dur)
            APP_ERRORS_TOTAL.labels(application=APP_NAME, error_type="TaskNotFoundException").inc()
            send_logstash_event(
                "ERROR",
                "Fallo crítico en TodoBook: intento de acceder o eliminar tarea inexistente (code_id=999)",
                "/api/error",
                {"error_code": "TODO_NOT_FOUND_999"},
            )
            body = json.dumps(
                {
                    "application": APP_NAME,
                    "status": "ERROR",
                    "error": "TaskNotFoundException: No existe la tarea con code_id=999",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        HTTP_REQUESTS_TOTAL.labels(application=APP_NAME, endpoint=path, method="GET", status="404").inc()
        self.send_response(404)
        self.end_headers()


if __name__ == "__main__":
    print(f"Iniciando {APP_NAME} en http://{HOST}:{PORT} (Metricas: /metrics, Logs TCP -> {LOGSTASH_HOST}:{LOGSTASH_PORT})", flush=True)
    send_logstash_event("INFO", f"Aplicación {APP_NAME} iniciada en el puerto {PORT}", "/startup")
    server = ThreadingHTTPServer((HOST, PORT), TodoMonitoringHandler)
    server.serve_forever()
