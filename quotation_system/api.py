from fastapi import FastAPI, HTTPException, Depends
from sqlmodel import Session, select
from typing import List
from .models import Quotation, QuotationCreate, QuotationRead, QuotationItem
from .db import get_session, create_db_and_tables, engine
from .pdf_maker import generate_pdf
import os

app = FastAPI(title="Bitumen Quotation System", version="1.0")

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

@app.post("/quotations/", response_model=QuotationRead)
def create_quotation(quotation: QuotationCreate, session: Session = Depends(get_session)):
    # 1. Convert Pydantic to SQLModel
    db_quote = Quotation.from_orm(quotation)
    
    # 2. Add Items
    for item in quotation.items:
        db_item = QuotationItem.from_orm(item)
        db_quote.items.append(db_item) # Check relationship handling
    
    # 3. Calculate Totals (Server-side validation)
    subtotal = sum(i.quantity * i.rate + i.freight_rate + i.packing_rate for i in quotation.items)
    # simplified logic for demo, ideally we calc item.total_amount first
    
    session.add(db_quote)
    session.commit()
    session.refresh(db_quote)
    return db_quote

@app.get("/quotations/{qt_id}", response_model=QuotationRead)
def read_quotation(qt_id: int, session: Session = Depends(get_session)):
    quote = session.get(Quotation, qt_id)
    if not quote:
        raise HTTPException(status_code=404, detail="Quotation not found")
    return quote

@app.get("/quotations/{qt_id}/pdf")
def get_quotation_pdf(qt_id: int, session: Session = Depends(get_session)):
    quote = session.get(Quotation, qt_id)
    if not quote:
        raise HTTPException(status_code=404, detail="Quotation not found")
        
    filename = f"Quote_{quote.quote_number}.pdf"
    full_path = os.path.abspath(filename)
    
    generate_pdf(quote, full_path)
    
    from fastapi.responses import FileResponse
    return FileResponse(full_path, media_type='application/pdf', filename=filename)

@app.get("/quotations/latest-number")
def get_latest_number(session: Session = Depends(get_session)):
    # Logic to find max number
    # Simple query
    statement = select(Quotation).order_by(Quotation.id.desc()).limit(1)
    result = session.exec(statement).first()
    if result:
        return {"latest_quote_no": result.quote_number}
    return {"latest_quote_no": "None"}
