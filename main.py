"""main.py — FastAPI service for PDF AcroForm filling.

Endpoints:
  GET  /health          — liveness probe
  POST /form/fill-pdf   — fill a form template with candidate data.
                          Returns binary PDF + fill-ratio headers:
                            Content-Type: application/pdf
                            Content-Disposition: attachment; filename="..."
                            X-Fill-Filled:   how many candidate fields had data
                            X-Fill-Total:    how many the form supports
                            X-Fill-Missing:  URL-encoded, comma-separated labels
                            X-Fill-Filename: same as Content-Disposition filename (n8n convenience)
"""
import logging
import urllib.parse
from datetime import date

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from form_agent import fill_form

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Form PDF Service", version="0.2.0")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "form-pdf-service"}


@app.post("/form/fill-pdf")
async def form_fill_pdf(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    form_id = body.get("form_id", "td63a")
    candidate = body.get("candidate", {})
    if not candidate:
        raise HTTPException(status_code=400, detail="candidate data is required")

    try:
        pdf_bytes, filled, total, missing = fill_form(form_id, candidate)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.exception("Form fill failed for %s", form_id)
        raise HTTPException(status_code=500, detail=str(e))

    name_slug = (candidate.get("name_en") or candidate.get("name_zh") or "unknown").replace(" ", "_")
    filename = f"{form_id.upper()}_{date.today()}_{name_slug}.pdf"
    missing_encoded = urllib.parse.quote(",".join(missing), safe=",")

    log.info(
        "Served %s (%d bytes) filled=%d/%d missing=[%s]",
        filename, len(pdf_bytes), filled, total, ",".join(missing),
    )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Fill-Filled": str(filled),
        "X-Fill-Total": str(total),
        "X-Fill-Missing": missing_encoded,
        "X-Fill-Filename": filename,
    }
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
