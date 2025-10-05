# test_base_model_full.py
import datetime as dt
import enum
import json
import uuid
from typing import Any, Dict, Optional

import pytest
from pydantic import BaseModel as PyBaseModel
from pydantic import Field, ValidationError

from rediskit.base_model import BaseModel, CustomJSONResponse, custom_json_deserializer, custom_json_serializer, json_encoder, serialize_values

# ---------------------------
# Test fixtures & helpers
# ---------------------------


class Color(enum.Enum):
    RED = 1
    GREEN = 2


class ArbitraryClass:
    def __init__(self, v: str) -> None:
        self.v = v


class Address(BaseModel):
    street: str | None = None
    city: Optional[str] = None
    meta: Dict[str, Any] = {}


class ExternalThing(PyBaseModel):
    a: int
    b: str


class User(BaseModel):
    # IDs, dates, enums
    id: uuid.UUID | None = None
    created_at: dt.datetime | None = None
    color: Optional[Color] = None

    # Simple types
    name: str | None = None
    age: Optional[int] = None

    # Collections
    tags: list[str] | None = None
    settings: dict[str, Any] | None = None

    # Nested model
    address: Optional[Address] = None

    # Alias field; populate_by_name=True should allow using "email" and alias
    email: Optional[str] = Field(default=None, alias="emailAddress")

    # Arbitrary types are allowed
    arbitrary: Optional[ArbitraryClass] = None


@pytest.fixture
def sample_user() -> User:
    return User(
        id=uuid.uuid4(),
        name="Ada",
        age=37,
        created_at=dt.datetime(2023, 1, 2, 3, 4, 5),
        color=Color.GREEN,
        tags=["alpha"],
        settings={"a": 1},
        address=Address(street="Baker St", city="London", meta={"zip": "NW1"}),
        email="ada@example.com",  # type: ignore # field name, not alias -> should work (populate_by_name)
        arbitrary=ArbitraryClass("x"),
    )


# ---------------------------
# dump()
# ---------------------------


def test_dump_keep_types_true(sample_user: User):
    data = sample_user.dump(keep_data_types=True, exclude_unset=False, exclude_none=False, enum_as_name=False)
    # model_dump returns native py types
    assert isinstance(data["id"], uuid.UUID)
    assert isinstance(data["created_at"], dt.datetime)
    assert data["color"] == Color.GREEN  # enum still an Enum when keep_data_types=True
    # nested model preserved as dict of native types
    assert isinstance(data["address"], dict)
    assert data["address"]["street"] == "Baker St"


def test_dump_keep_types_false_enum_value(sample_user: User):
    data = sample_user.dump(keep_data_types=False, exclude_unset=False, exclude_none=False, enum_as_name=False)
    assert isinstance(data["id"], str)
    assert data["created_at"].endswith("05")
    # enum -> value (not name)
    assert data["color"] == Color.GREEN.value
    assert data["address"]["meta"]["zip"] == "NW1"


def test_dump_keep_types_false_enum_name(sample_user: User):
    data = sample_user.dump(keep_data_types=False, exclude_unset=False, exclude_none=False, enum_as_name=True)
    assert data["color"] == "GREEN"


def test_dump_exclude_flags(sample_user: User):
    u = sample_user
    u.age = None
    data = u.dump(keep_data_types=True, exclude_unset=False, exclude_none=True)
    assert "age" not in data  # excluded because None
    data2 = u.dump(keep_data_types=True, exclude_unset=True, exclude_none=False)
    # All set fields remain (even if None) because exclude_none=False, exclude_unset=True
    assert "age" in data2


# ---------------------------
# merged_with() and update_from()
# ---------------------------


def test_merged_with_all_rules():
    u1 = User(
        id=uuid.uuid4(),
        name="Ada",
        created_at=dt.datetime(2020, 1, 1),
        color=Color.RED,
        tags=["alpha"],
        settings={"a": 1},
        address=Address(street="Baker St", city="London"),
    )
    u2 = User(
        id=u1.id,  # same type required
        name="Ada Lovelace",
        created_at=dt.datetime(2020, 1, 1),  # unchanged effectively
        color=Color.GREEN,
        tags=["alpha", "beta"],  # list union without duplicates
        settings={"b": 2},  # dict shallow union
        address=Address(city="Oxford"),  # nested merge: update city only
        age=None,  # None should be ignored (exclude_none=True default)
        email="ada@example.com",
    )

    merged = u1.merged_with(u2, exclude_unset=True, exclude_none=True)
    # list: keep unique, preserve original order then append new uniques
    assert merged.tags == ["alpha", "beta"]
    # dict: union
    assert merged.settings == {"a": 1, "b": 2}
    # nested: recurse
    assert merged.address.street == "Baker St"
    assert merged.address.city == "Oxford"
    # replacement for simple values
    assert merged.color == Color.GREEN
    assert merged.name == "Ada Lovelace"


def test_merged_with_type_mismatch():
    a = Address(street="x")
    b = User(id=uuid.uuid4(), name="n", created_at=dt.datetime.now())
    with pytest.raises(TypeError):
        a.merged_with(b)


def test_update_from_mutates_in_place():
    u1 = User(id=uuid.uuid4(), name="A", created_at=dt.datetime(2020, 1, 1), tags=["x"], settings={"a": 1}, address=Address(street="S", city="C"))
    u2 = User(id=u1.id, name="B", created_at=u1.created_at, tags=["x", "y"], settings={"b": 2}, address=Address(city="C2"))

    merged = u1.merged_with(u2, exclude_unset=True, exclude_none=True)
    u1.update_from(u2, exclude_unset=True, exclude_none=True)
    # same object updated to the merged state
    assert u1 == merged


def test_merge_respects_exclude_unset():
    u1 = User(id=uuid.uuid4(), name="A", created_at=dt.datetime(2020, 1, 1), age=20)
    # u2 doesn't set age; exclude_unset=True should keep existing
    u2 = User(id=u1.id, name="A2", created_at=u1.created_at)
    merged = u1.merged_with(u2, exclude_unset=True, exclude_none=True)
    assert merged.age == 20  # unchanged


def test_merge_none_keeps_current():
    u1 = User(id=uuid.uuid4(), name="A", created_at=dt.datetime(2020, 1, 1), age=30)
    u2 = User(id=u1.id, name="A", created_at=u1.created_at, age=None)
    merged = u1.merged_with(u2, exclude_unset=False, exclude_none=True)
    assert merged.age == 30


# ---------------------------
# safe_validate()
# ---------------------------


def test_safe_validate_sets_invalid_fields_to_none():
    bad_data = {
        "id": "not-a-uuid",
        "name": 123,  # wrong type
        "created_at": "2020-01-01T00:00:00",  # ok
        "age": "forty",  # wrong
    }
    u = User.safe_validate(bad_data)
    # invalid fields become None
    assert u.id is None
    assert u.name is None
    # valid parse should still work
    assert isinstance(u.created_at, dt.datetime)
    assert u.age is None


# ---------------------------
# json_encoder() and serialize_values()
# ---------------------------


def test_json_encoder_variants(sample_user: User):
    # enum value (default)
    assert json_encoder(Color.RED) == 1
    # enum by name
    assert json_encoder(Color.GREEN, enum_as_name=True) == "GREEN"
    # uuid
    uid = uuid.uuid4()
    assert json_encoder(uid) == str(uid)
    # datetime/date
    d = dt.date(2021, 2, 3)
    assert json_encoder(d) == d.isoformat()
    # BaseModel -> dump(keep_data_types=False)
    dumped = json_encoder(sample_user)
    assert isinstance(dumped["id"], str)
    # external pydantic BaseModel -> model_dump
    ext = ExternalThing(a=1, b="x")
    assert json_encoder(ext) == {"a": 1, "b": "x"}

    # raiseIfNoMatch
    class NoJSON: ...

    with pytest.raises(TypeError):
        json_encoder(NoJSON(), raiseIfNoMatch=True)


def test_serialize_values_on_nested_structures():
    data = {
        Color.RED: [uuid.UUID(int=1), dt.date(2020, 1, 1)],
        "k2": {"when": dt.datetime(2020, 1, 1, 2, 3, 4)},
    }
    out = serialize_values(data)
    # enum keys become values by default (1)
    assert "1" in out or 1 in out  # dict keys are stringified by json.dumps later
    key = "1" if "1" in out else 1
    assert isinstance(out[key][0], str)  # uuid string
    assert out["k2"]["when"].startswith("2020-01-01T02:03:04")


def test_serialize_values_enum_as_name():
    data = {Color.GREEN: "ok"}
    out = serialize_values(data, enum_as_name=True)
    assert "GREEN" in out


# ---------------------------
# Custom JSON serializer/deserializer & response
# ---------------------------


def test_custom_json_serializer_and_deserializer(sample_user: User):
    sample_user.arbitrary = None
    payload = {
        "msg": "hi",
        "user": sample_user,
        "ext": ExternalThing(a=2, b="bb"),
        "emoji": "😊",  # ensure non-ascii handled
    }
    s = custom_json_serializer(payload)
    # should be valid JSON and include non-ASCII intact (ensure_ascii=False in CustomJSONResponse;
    # custom_json_serializer uses serialize_values -> json.dumps default ensures ascii but we check content round-trip)
    obj = custom_json_deserializer(s)
    assert obj["msg"] == "hi"
    assert obj["emoji"] == "😊"
    assert isinstance(obj["user"]["id"], str)
    assert obj["ext"] == {"a": 2, "b": "bb"}


def test_CustomJSONResponse_render(sample_user: User):
    sample_user.arbitrary = None
    content = {
        "n": 1,
        "u": sample_user,
        "non_ascii": "Østergård",
    }
    resp = CustomJSONResponse(content=content)
    rendered = resp.render(content)
    # bytes
    assert isinstance(rendered, (bytes, bytearray))
    decoded = rendered.decode("utf-8")
    # Valid JSON and non-ascii preserved (ensure_ascii=False)
    parsed = json.loads(decoded)
    assert parsed["non_ascii"] == "Østergård"
    assert isinstance(parsed["u"]["id"], str)
    assert parsed["u"]["address"]["street"] == "Baker St"


# ---------------------------
# Pydantic config behaviors
# ---------------------------


def test_validate_assignment_enforced(sample_user: User):
    # Changing to a wrong type should raise ValidationError (validate_assignment=True)
    with pytest.raises(ValidationError):
        sample_user.age = "not-an-int"  # type: ignore


def test_populate_by_name_allows_field_name_instead_of_alias():
    # Provided "email" instead of alias "emailAddress"
    u = User(
        id=uuid.uuid4(),
        name="N",
        created_at=dt.datetime.now(),
        email="x@x.com",
    )
    assert u.email == "x@x.com"
    # Also allow population by alias
    u2 = User(
        id=u.id,
        name="N2",
        created_at=u.created_at,
        emailAddress="alias@x.com",
    )
    assert u2.email == "alias@x.com"


def test_arbitrary_types_allowed_field():
    u = User(
        id=uuid.uuid4(),
        name="N",
        created_at=dt.datetime.now(),
        arbitrary=ArbitraryClass("ok"),
    )
    assert isinstance(u.arbitrary, ArbitraryClass)


# ---------------------------
# _merge_value() list and dict specifics (direct)
# ---------------------------


def test_merge_value_list_dedup_and_order():
    u = User(id=uuid.uuid4(), name="A", created_at=dt.datetime.now(), tags=["a", "b"])
    incoming = User(id=u.id, name="A", created_at=u.created_at, tags=["b", "c", "a", "d"])
    merged = u.merged_with(incoming, exclude_unset=True, exclude_none=True)
    # start with current, then add items not present in current
    assert merged.tags == ["a", "b", "c", "d"]


def test_merge_value_dict_union():
    u = User(id=uuid.uuid4(), name="A", created_at=dt.datetime.now(), settings={"x": 1, "y": 2})
    incoming = User(id=u.id, name="A", created_at=u.created_at, settings={"y": 20, "z": 3})
    merged = u.merged_with(incoming, exclude_unset=True, exclude_none=True)
    assert merged.settings == {"x": 1, "y": 20, "z": 3}
