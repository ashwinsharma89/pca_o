#!/usr/bin/env python3
import os
import sys
import subprocess
import signal
import time
import argparse
from pathlib import Path
from typing import List, Optional

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.resolve()
BACKEND_LOG = PROJECT_ROOT / "logs" / "backend.log"
FRONTEND_LOG = PROJECT_ROOT / "logs" / "frontend.log"
VENV_DIR = PROJECT_ROOT / ".venv312"

# Color constants for logging
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def log(message: str, color: str = Colors.OKBLUE):
    print(f"{color}{message}{Colors.ENDC}")

def get_venv_python() -> Path:
    """Find the correct python executable in the venv across platforms."""
    if os.name == "nt":  # Windows
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"

def kill_port(port: int):
    """Cross-platform port clearing."""
    log(f"Checking for processes on port {port}...", Colors.OKCYAN)
    if os.name == "nt":
        # Windows approach using netstat and taskkill
        try:
            cmd = f'netstat -ano | findstr :{port}'
            output = subprocess.check_output(cmd, shell=True).decode()
            for line in output.splitlines():
                if "LISTENING" in line:
                    pid = line.strip().split()[-1]
                    log(f"Killing process {pid} on port {port}", Colors.WARNING)
                    subprocess.run(['taskkill', '/F', '/PID', pid], check=False)
        except Exception:
            pass
    else:
        # Mac/Linux approach using lsof
        try:
            cmd = f'lsof -ti :{port}'
            pids = subprocess.check_output(cmd, shell=True).decode().split()
            for pid in pids:
                log(f"Killing process {pid} on port {port}", Colors.WARNING)
                os.kill(int(pid), signal.SIGKILL)
        except Exception:
            pass

def start_backend():
    """Start the FastAPI backend."""
    log("🚀 Starting Backend API...", Colors.BOLD)
    python_exe = get_venv_python()
    if not python_exe.exists():
        log(f"Error: Virtual environment not found at {VENV_DIR}", Colors.FAIL)
        return None

    PROJECT_ROOT.joinpath("logs").mkdir(exist_ok=True)
    
    # Environment variables
    env = os.environ.copy()
    env["API_PORT"] = "8001"
    env["PYTHONPATH"] = str(PROJECT_ROOT)

    with open(BACKEND_LOG, "w") as log_file:
        proc = subprocess.Popen(
            [str(python_exe), "-m", "src.interface.api.main"],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=None if os.name == "nt" else os.setpgrp
        )
    log(f"Backend started (PID: {proc.pid})", Colors.OKGREEN)
    return proc

def start_frontend():
    """Start the Next.js frontend."""
    log("🏗️ Starting Frontend...", Colors.BOLD)
    frontend_dir = PROJECT_ROOT / "frontend"
    
    env = os.environ.copy()
    env["PORT"] = "3000"

    with open(FRONTEND_LOG, "w") as log_file:
        # Use shell=True for npm to handle PATH issues on Windows
        proc = subprocess.Popen(
            "npm run dev",
            cwd=frontend_dir,
            env=env,
            shell=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=None if os.name == "nt" else os.setpgrp
        )
    log(f"Frontend started (PID: {proc.pid})", Colors.OKGREEN)
    return proc

def cleanup_and_exit(signum, frame):
    log("\n🛑 Stopping services...", Colors.WARNING)
    # This script kills its own children because they are in the same process group (on Unix)
    if os.name != "nt":
        os.killpg(0, signal.SIGTERM)
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="PCA Agent Management Tool")
    parser.add_argument("command", choices=["start", "stop", "setup"], help="Action to perform")
    args = parser.parse_args()

    if args.command == "start":
        kill_port(3000)
        kill_port(8001)
        
        backend = start_backend()
        frontend = start_frontend()

        log("\n✅ All services initiated. Press Ctrl+C to stop.", Colors.BOLD + Colors.OKGREEN)
        log(f"Backend Logs: {BACKEND_LOG}")
        log(f"Frontend Logs: {FRONTEND_LOG}")

        signal.signal(signal.SIGINT, cleanup_and_exit)
        
        while True:
            time.sleep(1)
    
    elif args.command == "stop":
        kill_port(3000)
        kill_port(8001)
        log("Services stopped.", Colors.OKGREEN)

    elif args.command == "setup":
        log("📦 Setting up environment...", Colors.BOLD)
        
        # 1. Backend setup
        log("\n--- Backend Setup ---", Colors.OKCYAN)
        python_exe = get_venv_python()
        if not python_exe.exists():
            log(f"Creating virtual environment in {VENV_DIR}...", Colors.OKBLUE)
            subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
            python_exe = get_venv_python()

        log("Installing Python dependencies...", Colors.OKBLUE)
        subprocess.run([str(python_exe), "-m", "pip", "install", "-r", "requirements.txt"], check=True)

        # 2. Frontend setup
        log("\n--- Frontend Setup ---", Colors.OKCYAN)
        frontend_dir = PROJECT_ROOT / "frontend"
        log("Installing NPM packages...", Colors.OKBLUE)
        subprocess.run("npm install", cwd=frontend_dir, shell=True, check=True)

        log("\n✅ Setup complete! You can now run: python manage.py start", Colors.OKGREEN + Colors.BOLD)

if __name__ == "__main__":
    main()
