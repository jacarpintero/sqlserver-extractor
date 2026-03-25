"""
API Server (FastAPI) para el EC2.
Recibe triggers del equipo externo o de pipeline_windows y ejecuta el ETL.

Endpoints:
    POST /trigger  - inicia el pipeline ETL para un cliente
    GET  /status/{job_id} - consulta el estado de un job
    GET  /health   - healthcheck

Uso:
    uvicorn api.api_server:app --host 0.0.0.0 --port 8000
"""

import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime
from typing import Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

load_dotenv()

logger = logging.getLogger("api_server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI(title="ETL Pipeline API", version="1.0.0")

# Almacen en memoria de jobs (en produccion usar Redis o BD)
jobs: Dict[str, dict] = {}

API_TOKEN = os.getenv("EC2_API_TOKEN", "")


def verify_token(authorization: Optional[str]):
    if not API_TOKEN:
        return  # Sin token configurado, acceso abierto
    if not authorization or authorization != "Bearer {}".format(API_TOKEN):
        raise HTTPException(status_code=401, detail="Token invalido o ausente")


class TriggerRequest(BaseModel):
    client: str
    monthly_period: Optional[str] = None  # Si None, auto-detecta desde S3
    weekly_period: Optional[str] = None   # Si None, auto-detecta desde S3
    dry_run: bool = False


class TriggerResponse(BaseModel):
    job_id: str
    status: str
    message: str


class StatusResponse(BaseModel):
    job_id: str
    status: str
    client: str
    started_at: str
    finished_at: Optional[str]
    message: str


def run_pipeline_background(job_id: str, request: TriggerRequest):
    """Corre el pipeline EC2 en background y actualiza el estado del job."""
    jobs[job_id]["status"] = "running"

    cmd = [
        sys.executable, "-m", "pipeline.pipeline_ec2",
        "--client", request.client,
    ]
    if request.monthly_period and request.monthly_period != "auto":
        cmd += ["--monthly-period", request.monthly_period]
    if request.weekly_period and request.weekly_period != "auto":
        cmd += ["--weekly-period", request.weekly_period]
    if request.dry_run:
        cmd.append("--dry-run")

    logger.info("Job {}: ejecutando {}".format(job_id, " ".join(cmd)))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            jobs[job_id]["status"] = "success"
            jobs[job_id]["message"] = "Pipeline completado exitosamente"
        else:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["message"] = result.stderr[-500:] if result.stderr else "Error desconocido"
            if result.stderr:
                logger.error(
                    "Job {} pipeline stderr (ultimos 8k):\n{}".format(
                        job_id, result.stderr[-8000:]
                    )
                )
            if result.stdout:
                logger.error(
                    "Job {} pipeline stdout (ultimos 8k):\n{}".format(
                        job_id, result.stdout[-8000:]
                    )
                )
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["message"] = str(e)
    finally:
        jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()
        logger.info("Job {} finalizado con status: {}".format(job_id, jobs[job_id]["status"]))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/trigger", response_model=TriggerResponse)
def trigger(request: TriggerRequest, authorization: Optional[str] = Header(None)):
    verify_token(authorization)

    # Verificar que no haya un job corriendo para el mismo cliente
    for job in jobs.values():
        if job["client"] == request.client and job["status"] == "running":
            raise HTTPException(
                status_code=409,
                detail="Ya hay un job corriendo para el cliente {}".format(request.client),
            )

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "client": request.client,
        "status": "queued",
        "started_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "message": "Job en cola",
    }

    # Correr en background (thread simple)
    import threading
    thread = threading.Thread(
        target=run_pipeline_background,
        args=(job_id, request),
        daemon=True,
    )
    thread.start()

    return TriggerResponse(
        job_id=job_id,
        status="queued",
        message="Pipeline iniciado para cliente {}".format(request.client),
    )


@app.get("/status/{job_id}", response_model=StatusResponse)
def status(job_id: str, authorization: Optional[str] = Header(None)):
    verify_token(authorization)

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    job = jobs[job_id]
    return StatusResponse(
        job_id=job_id,
        status=job["status"],
        client=job["client"],
        started_at=job["started_at"],
        finished_at=job.get("finished_at"),
        message=job["message"],
    )


@app.get("/jobs")
def list_jobs(authorization: Optional[str] = Header(None)):
    verify_token(authorization)
    return {"jobs": jobs}
