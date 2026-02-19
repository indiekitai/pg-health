"""Web UI for PG Health."""

import json
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .checks import run_health_check
from .models import Severity

app = FastAPI(title="PG Health", description="PostgreSQL health check tool")

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Home page with connection form."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/check", response_class=HTMLResponse)
async def check(
    request: Request,
    connection_string: Annotated[str, Form()],
):
    """Run health check and return results."""
    
    try:
        report = await run_health_check(connection_string)
        return templates.TemplateResponse(
            "report.html",
            {
                "request": request,
                "report": report,
                "Severity": Severity,
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error": str(e),
            },
        )


def run():
    """Run the web server."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8767)


if __name__ == "__main__":
    run()
