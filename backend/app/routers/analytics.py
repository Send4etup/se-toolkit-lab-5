"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    from sqlalchemy import case, cast, Date, func
    from sqlmodel import select
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.like(lab_pattern))
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
    )
    task_ids = [t.id for t in result.all()]

    counts = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    if task_ids:
        bucket_expr = case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100",
        )
        result = await session.exec(
            select(bucket_expr, func.count()).where(
                InteractionLog.item_id.in_(task_ids), InteractionLog.score.is_not(None)
            ).group_by(bucket_expr)
        )
        for bucket, count in result.all():
            counts[bucket] = count

    return [{"bucket": b, "count": c} for b, c in counts.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    from sqlalchemy import func
    from sqlmodel import select
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.like(lab_pattern))
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
        .order_by(ItemRecord.title)
    )
    tasks = result.all()

    output = []
    for task in tasks:
        result = await session.exec(
            select(func.round(func.avg(InteractionLog.score), 1), func.count())
            .where(InteractionLog.item_id == task.id)
        )
        row = result.first()
        output.append({
            "task": task.title,
            "avg_score": float(row[0]) if row[0] is not None else 0.0,
            "attempts": row[1] or 0,
        })

    return output


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    from sqlalchemy import func
    from sqlmodel import select
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.like(lab_pattern))
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
    )
    task_ids = [t.id for t in result.all()]
    if not task_ids:
        return []

    date_expr = func.date(InteractionLog.created_at)
    result = await session.exec(
        select(date_expr, func.count())
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )
    return [{"date": str(row[0]), "submissions": row[1]} for row in result.all()]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_num = lab.split("-")[-1]
    lab_pattern = f"Lab {lab_num}%"

    from sqlalchemy import func
    from sqlmodel import select
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.like(lab_pattern))
    )
    lab_item = result.first()
    if not lab_item:
        return []

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
    )
    task_ids = [t.id for t in result.all()]
    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(func.distinct(InteractionLog.learner_id)),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    return [{"group": r[0], "avg_score": float(r[1]), "students": r[2]} for r in result.all()]