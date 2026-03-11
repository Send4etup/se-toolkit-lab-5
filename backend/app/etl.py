"""ETL pipeline: fetch data from the autochecker API and load it into the database."""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


async def fetch_items() -> list[dict]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        if response.status_code != 200:
            raise Exception(f"Failed to fetch items: {response.status_code} {response.text}")
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    all_logs = []
    params = {"limit": 500}
    if since:
        params["since"] = since.isoformat()

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code} {response.text}")
            data = response.json()
            logs = data["logs"]
            all_logs.extend(logs)
            if not data["has_more"] or not logs:
                break
            last_submitted_at = logs[-1]["submitted_at"]
            params["since"] = last_submitted_at

    return all_logs


async def load_items(items: list[dict], session: AsyncSession) -> int:
    new_count = 0
    lab_map = {}

    labs = [i for i in items if i["type"] == "lab"]
    tasks = [i for i in items if i["type"] == "task"]

    for lab in labs:
        result = await session.exec(
            select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title == lab["title"])
        )
        existing = result.first()
        if existing:
            lab_map[lab["lab"]] = existing
        else:
            new_lab = ItemRecord(type="lab", title=lab["title"])
            session.add(new_lab)
            await session.flush()
            lab_map[lab["lab"]] = new_lab
            new_count += 1

    for task in tasks:
        parent = lab_map.get(task["lab"])
        if not parent:
            continue
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task["title"],
                ItemRecord.parent_id == parent.id,
            )
        )
        existing = result.first()
        if not existing:
            new_task = ItemRecord(type="task", title=task["title"], parent_id=parent.id)
            session.add(new_task)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    new_count = 0

    title_lookup = {}
    for item in items_catalog:
        key = (item["lab"], item["task"])
        title_lookup[key] = item["title"]

    for log in logs:
        # 1. Find or create learner
        result = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = result.first()
        if not learner:
            learner = Learner(external_id=log["student_id"], student_group=log["group"])
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        title = title_lookup.get((log["lab"], log["task"]))
        if not title:
            continue
        result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == title)
        )
        item = result.first()
        if not item:
            continue

        # 3. Idempotency check
        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        if result.first():
            continue

        # 4. Create interaction
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00")).replace(tzinfo=None),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


async def sync(session: AsyncSession) -> dict:
    # Step 1: fetch and load items
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: determine last sync timestamp
    result = await session.exec(
        select(InteractionLog.created_at).order_by(InteractionLog.created_at.desc())
    )
    last_created_at = result.first()

    # Step 3: fetch and load logs
    logs = await fetch_logs(since=last_created_at)
    new_records = await load_logs(logs, items_catalog, session)

    # Step 4: count total
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}