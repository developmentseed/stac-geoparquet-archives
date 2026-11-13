import json
from pathlib import Path

import pytest
from pystac import Catalog, Collection

CATALOG_PATH = Path(__file__).parents[1] / "catalog" / "catalog.json"
EXTERNAL_RELS = {"license", "about", "cite-as"}


def read_catalog() -> Catalog:
    return Catalog.from_file(CATALOG_PATH)


def read_collections() -> list[Collection]:
    return list(read_catalog().get_all_collections())


@pytest.fixture
def catalog() -> Catalog:
    return read_catalog()


def get_collection(catalog: Catalog, collection_id: str) -> Collection:
    collection = catalog.get_child(collection_id)
    assert isinstance(collection, Collection)
    return collection


def test_validate_all(catalog: Catalog) -> None:
    catalog.validate_all()


def test_all_json_files_are_linked() -> None:
    hrefs = {o.get_self_href() for o in [read_catalog(), *read_collections()]}
    linked = {Path(href).resolve() for href in hrefs if href is not None}
    assert set(CATALOG_PATH.parent.resolve().rglob("*.json")) == linked


@pytest.mark.parametrize("collection", read_collections(), ids=lambda c: c.id)
def test_collection_id_matches_directory(collection: Collection) -> None:
    href = collection.get_self_href()
    assert href is not None
    assert Path(href).parent.name == collection.id


@pytest.mark.parametrize(
    "stac_object", [read_catalog(), *read_collections()], ids=lambda o: o.id
)
def test_links_are_relative(stac_object: Catalog) -> None:
    href = stac_object.get_self_href()
    assert href is not None
    for link in json.loads(Path(href).read_text())["links"]:
        assert link["rel"] != "self"
        if link["rel"] not in EXTERNAL_RELS:
            assert "://" not in link["href"], link
