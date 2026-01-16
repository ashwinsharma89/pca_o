import socket
import requests
import sys
from rich.console import Console
from rich.table import Table

console = Console()

SERVICES = [
    {"name": "Prometheus", "host": "localhost", "port": 9090, "url": "http://localhost:9090"},
    {"name": "Grafana", "host": "localhost", "port": 3001, "url": "http://localhost:3001"},
    {"name": "Redis", "host": "localhost", "port": 6379, "url": None},
    {"name": "Flower (Celery)", "host": "localhost", "port": 5540, "url": "http://localhost:5540"},
    {"name": "API Server", "host": "127.0.0.1", "port": 8000, "url": "http://localhost:8000/health"},
]

def check_port(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def main():
    table = Table(title="PCA Agent Observability Status")
    table.add_column("Service", style="cyan")
    table.add_column("Endpoint", style="magenta")
    table.add_column("Status", style="bold")

    for svc in SERVICES:
        is_up = check_port(svc["host"], svc["port"])
        status = "[green]ONLINE[/green]" if is_up else "[red]OFFLINE[/red]"
        endpoint = f"{svc['host']}:{svc['port']}"
        table.add_row(svc["name"], endpoint, status)

    console.print(table)
    
    console.print("\n[bold]Loguru Logs:[/bold] tail -f logs/server.log")
    console.print("[bold]Dashboards:[/bold]  open ops/mission_control.html")

if __name__ == "__main__":
    main()
