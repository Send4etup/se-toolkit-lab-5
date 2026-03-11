"""Router for analytics endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def lab_title_filter(lab: str) -> str:
    parts = lab.split("-")
    if len(parts) == 2:
        return f"Lab {parts[1].lstrip('0') or '0'}"
    return lab


def lab_title_like(lab: str) -> str:
    parts = lab.split("-")
    if len(parts) == 2:
        num = parts[1]
        return f"Lab {num}"
    return lab


@router.get("/scores")
async def get_scores(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.like(lab_pattern),
        )
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    tasks = result.all()
    task_ids = [t.id for t in tasks]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    result = await session.exec(
        select(bucket_expr, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_expr)
    )
    rows = result.all()

    counts = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for bucket, count in rows:
        counts[bucket] = count

    return [{"bucket": b, "count": c} for b, c in counts.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.like(lab_pattern),
        )
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    tasks = result.all()

    output = []
    for task in sorted(tasks, key=lambda t: t.title):
        result = await session.exec(
            select(
                func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
                func.count().label("attempts"),
            ).where(InteractionLog.item_id == task.id)
        )
        row = result.first()
        avg_score = float(row[0]) if row[0] is not None else 0.0
        attempts = row[1] or 0
        output.append({"task": task.title, "avg_score": avg_score, "attempts": attempts})

    return output

@router.get("/timeline")
async def get_timeline(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.like(lab_pattern),
        )
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    tasks = result.all()
    task_ids = [t.id for t in tasks]

    if not task_ids:
        return []

    date_expr = func.date(InteractionLog.created_at)

    result = await session.exec(
        select(date_expr.label("date"), func.count().label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )
    rows = result.all()

    return [{"date": str(row[0]), "submissions": row[1]} for row in rows]

@router.get("/groups")
async def get_groups(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.like(lab_pattern),
        )
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    tasks = result.all()
    task_ids = [t.id for t in tasks]

    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    rows = result.all()

    return [
        {"group": row[0], "avg_score": float(row[1]), "students": row[2]}
        for row in rows
    ]