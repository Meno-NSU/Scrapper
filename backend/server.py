from fastapi import FastAPI, Query, HTTPException, status
from pydantic import BaseModel
from datetime import date, datetime
from typing import List, Optional

from backend.repo import NewsRepository

app = FastAPI(title="Knowledge Base Scrapper API", version="1.0.0")
repo = NewsRepository()


class NewsDocumentResponse(BaseModel):
    document_id: int
    content: str
    url: str
    doc_date: datetime


@app.get(
    "/meno/scrapper/news",
    response_model=List[NewsDocumentResponse],
    status_code=status.HTTP_200_OK,
    summary="Получить очищенные новости по дате публикации",
)
async def get_updated_news(
    from_date: Optional[date] = Query(
        default=None,
        alias="from",
        description="Дата в формате YYYY-MM-DD (например, 2026-05-22). Если не указана — вернутся все данные.",
    ),
):
    try:
        from_time = None
        if from_date is not None:
            from_time = datetime.combine(from_date, datetime.min.time())

        data = repo.get_updated_news(from_time)
        return data
    except Exception as e:
        print(f"[API Error] Ошибка: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера.",
        )