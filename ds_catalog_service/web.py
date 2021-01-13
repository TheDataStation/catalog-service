from typing import Optional

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr, Json
from pydantic.typing import Enum
from datetime import datetime
from sqlite3 import connect
from pathlib import Path

DB_PATH = Path.home().joinpath('catalog.db')


########## MODELS ##########
class UserPermissions(Enum):
    read = "READ"
    write = "WRITE"
    admin = "ADMIN"


class User(BaseModel):
    id: str
    name: Optional[str] = None
    permission: Optional[UserPermissions] = UserPermissions.admin
    email: Optional[EmailStr]


class DataAsset(BaseModel):
    id: str
    owner_id: str = None
    name: Optional[str]
    metadata: Optional[Json] = "\"{}\""
    created_at: Optional[datetime] = datetime.utcnow()


class Profiling(BaseModel):
    id: str
    source_id: str
    who_id: str
    started_at: Optional[datetime] = datetime.utcnow()
    source_schema: Optional[str]
    source_schema_retrieved_at: Optional[datetime]
    description: str
    metadata: Optional[Json] = "\"{}\""
    finished_at: Optional[datetime]


app = FastAPI(title="Data Catalog Service Web API",
              description="Can be used to manage users, data assets and bookkeep profiling processes")


@app.get("/")
def read_root():
    return RedirectResponse(url=app.docs_url)


@app.get("/statistics")
def statistics():
    return {"message": "some basic statistics go here"}


@app.post("/users/")
def create_user(user: User):
    with connect(DB_PATH) as db:
        db.execute("insert or replace into user values (?, ?, ?, ?)",
                   (user.id, user.name, str(user.permission.value), user.email))
    return user


@app.get("/users/")
def get_all_users():
    with connect(DB_PATH) as db:
        users = [User(id=id, name=name, permission=permission, email=email) for id, name, permission, email in
                 db.execute("select * from user")]
    return users


@app.get("/users/{id}")
def get_user(id: str):
    with connect(DB_PATH) as db:
        users = [User(id=id, name=name, permission=permission, email=email) for id, name, permission, email in
                 db.execute("select * from user where id=?", (id,))]

    return users[0]


@app.post("/data-assets/")
def create_data_asset(data_asset: DataAsset):
    with connect(DB_PATH) as db:
        db.execute("insert or replace into data_asset values (?, ?, ?, ?, ?)",
                   (data_asset.id, data_asset.owner_id, data_asset.name, str(data_asset.metadata),
                    data_asset.created_at))
    return data_asset


@app.get("/data-assets/{data_asset_id}")
def get_data_asset(data_asset_id: str):
    with connect(DB_PATH) as db:
        results = [DataAsset(id=id, name=name, metadata=metadata, created_at=created_at)
                   for id, owner_id, name, metadata, created_at in
                   db.execute("select * from data_asset where id=?", (data_asset_id,))]

    return results[0]


@app.get("/data-assets/")
def search_data_assets(q: str = "*"):
    with connect(DB_PATH) as db:
        results = [DataAsset(id=id, name=name, metadata=metadata, created_at=created_at)
                   for id, owner_id, name, metadata, created_at in
                   db.execute("select * from data_asset where name like ?", ('%' + q + '%',))]

    return results


@app.post("/profilings/")
def create_profiling(profiling: Profiling):
    with connect(DB_PATH) as db:
        db.execute("insert or replace into profiling values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (profiling.id, profiling.source_id, profiling.who_id, profiling.started_at,
                    profiling.source_schema, profiling.source_schema_retrieved_at, profiling.description,
                    profiling.metadata, profiling.finished_at))
    return profiling


@app.get("/profilings/{profiling_id}")
def get_profiling(profiling_id: str):
    with connect(DB_PATH) as db:
        results = [Profiling(id, source_id, who_id, started_at, source_schema, source_schema_retrieved_at, description,
                             metadata, finished_at)
                   for
                   id, source_id, who_id, started_at, source_schema, source_schema_retrieved_at, description, metadata, finished_at
                   in
                   db.execute("select * from profiling where id=?", (profiling_id,))]

    return results[0]
