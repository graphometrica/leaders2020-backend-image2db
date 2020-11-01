from pathlib import Path

import requests
from sqlalchemy import Column, Table, create_engine, insert
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import MetaData
from sqlalchemy.types import Integer, String

engine = create_engine(
    "postgresql://graph:graph@23.251.145.120:5432/animal",
    pool_pre_ping=True,
)
session_maker = sessionmaker(bind=engine)
session = session_maker()
meta = MetaData(bind=engine, schema="animal_schema")


data_img_tbl = Table(
    "images_data",
    meta,
    Column("image_id", Integer, primary_key=True),
    Column("file_prefix", String, nullable=False),
    Column("folder_prefix", String, nullable=False),
)

data_img_tbl.drop(engine, checkfirst=True)
data_img_tbl.create(engine, checkfirst=True)


if __name__ == "__main__":
    root = Path(__file__).parent.joinpath("images")

    for item in root.glob("*"):
        print(f"In folder {item.name}...")
        file_list = list(item.glob("*"))

        ids = [
            int(requests.post(
                "http://35.199.48.193:7001/save_image_to_db",
                files={f.name: f.open("rb")}
            ).json()["ids"][0]) for f in file_list
        ]
        print(f"Inserted {len(ids)} images to db")

        data = [
            {"image_id": id, "file_prefix": f.name, "folder_prefix": item.name}
            for f, id in zip(file_list, ids)
        ]

        insert_q = insert(data_img_tbl, values=data)
        session.execute(insert_q)
        session.commit()

        print(f"Done in {item.name}.")
