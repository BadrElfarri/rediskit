import uuid
from datetime import date, datetime, timedelta
from typing import Literal

import pytest
from pydantic import ValidationError

from rediskit.typed_fields.base_field import build_dynamic_model
from rediskit.typed_fields.bool_field import BoolField
from rediskit.typed_fields.date_field import DateField, DateTimeField
from rediskit.typed_fields.email_field import EmailField
from rediskit.typed_fields.enum_field import EnumField
from rediskit.typed_fields.float_field import FloatField
from rediskit.typed_fields.from_func import build_model_from_function
from rediskit.typed_fields.int_field import IntField
from rediskit.typed_fields.json_field import JsonField
from rediskit.typed_fields.list_field import ListField
from rediskit.typed_fields.object_field import ObjectField
from rediskit.typed_fields.str_field import StrField
from rediskit.typed_fields.uuid_field import UuidField


def test_str_field_constraints():
    field = StrField(label="name", min_length=2, max_length=5, default_str="abc")
    model_cls = build_dynamic_model("StrModel", [field])
    obj = model_cls(name="test")
    assert obj.name == "test"

    with pytest.raises(ValidationError):
        model_cls(name="a")
    with pytest.raises(ValidationError):
        model_cls(name="toolong")


def test_int_field_bounds():
    field = IntField(label="age", ge_int=18, le_int=65, default_int=30)
    model_cls = build_dynamic_model("IntModel", [field])
    obj = model_cls(age=25)
    assert obj.age == 25

    with pytest.raises(ValidationError):
        model_cls(age=10)
    with pytest.raises(ValidationError):
        model_cls(age=100)


def test_float_field_bounds():
    field = FloatField(label="score", ge_float=0.0, le_float=1.0, default_float=0.5)
    model_cls = build_dynamic_model("FloatModel", [field])
    obj = model_cls(score=0.75)
    assert obj.score == 0.75

    with pytest.raises(ValidationError):
        model_cls(score=1.5)


def test_boolean_field():
    field = BoolField(label="active", default_bool=True)
    model_cls = build_dynamic_model("BoolModel", [field])
    obj = model_cls(active=False)
    assert obj.active is False


def test_date_field():
    today = date.today()
    field = DateField(label="created", default_date=today)
    model_cls = build_dynamic_model("DateModel", [field])
    obj = model_cls(created=today)
    assert obj.created == today


def test_datetime_field():
    now = datetime.utcnow()
    field = DateTimeField(label="timestamp", default_datetime=now)
    model_cls = build_dynamic_model("DateTimeModel", [field])
    obj = model_cls(timestamp=now)
    assert obj.timestamp == now


def test_json_field():
    value = {"key": "value"}
    field = JsonField(label="config", default_dict=value)
    model_cls = build_dynamic_model("JsonModel", [field])
    obj = model_cls(config=value)
    assert obj.config == value


def test_list_field():
    value = [1, 2, 3]
    field = ListField(label="items", default_list=value)
    model_cls = build_dynamic_model("ListModel", [field])
    obj = model_cls(items=value)
    assert obj.items == value


def test_email_field():
    email = "user@example.com"
    field = EmailField(label="email", default_email=email)
    model_cls = build_dynamic_model("EmailModel", [field])
    obj = model_cls(email=email)
    assert isinstance(obj.email, str)

    with pytest.raises(ValidationError):
        model_cls(email="not-an-email")


def test_uuid_field():
    uid = uuid.uuid4()
    field = UuidField(label="identifier", default_uuid=uid)
    model_cls = build_dynamic_model("UuidModel", [field])
    obj = model_cls(identifier=uid)
    assert obj.identifier == uid

    with pytest.raises(ValidationError):
        model_cls(identifier="invalid-uuid")


def test_enum_field():
    options = ["red", "green", "blue"]
    field = EnumField(label="color", allowed_values=options, default_str="red")
    model_cls = build_dynamic_model("EnumModel", [field])
    obj = model_cls(color="green")
    assert obj.color.value == "green"

    with pytest.raises(ValidationError):
        model_cls(color="yellow")


def test_nested_object_field():
    inner_fields = [
        StrField(label="first_name", min_length=1, max_length=50, default_str="John"),
        StrField(label="last_name", min_length=1, max_length=50, default_str="Doe"),
        EmailField(label="contact_email", default_email="john.doe@example.com"),
    ]

    nested = ObjectField(label="user", fields=inner_fields)
    model_cls = build_dynamic_model("NestedUserModel", [nested])

    instance = model_cls(user={"first_name": "John", "last_name": "Doe", "contact_email": "john.doe@example.com"})

    assert instance.user.first_name == "John"
    assert instance.user.last_name == "Doe"
    assert instance.user.contact_email == "john.doe@example.com"


def test_combined_model_multiple_field_types():
    now = datetime.utcnow()
    today = date.today()
    uid = uuid.uuid4()

    fields = [
        StrField(
            label="name",
            min_length=2,
            max_length=10,
            default_str="Jane",
            description="The user's given name",
        ),
        IntField(label="age", ge_int=0, le_int=120, default_int=28),
        FloatField(label="rating", ge_float=0.0, le_float=5.0, default_float=4.5),
        BoolField(label="active", default_bool=True),
        DateField(label="birthdate", default_date=today),
        DateTimeField(label="joined_at", default_datetime=now),
        JsonField(label="preferences", default_dict={"theme": "dark"}),
        ListField(label="tags", default_list=["dev", "test"]),
        EmailField(label="email", default_email="example@test.com"),
        UuidField(label="user_id", default_uuid=uid),
        EnumField(label="status", allowed_values=["new", "active", "disabled"], default_str="active"),
        ObjectField(
            label="profile",
            fields=[
                StrField(label="first_name", default_str="John"),
                StrField(label="last_name", default_str="Doe"),
                EmailField(label="contact_email", default_email="john.doe@example.com"),
            ],
        ),
        EmailField(label="email", default_email="example@test.com"),
        UuidField(label="user_id", default_uuid=uid),
        EnumField(label="status", allowed_values=["new", "active", "disabled"], default_str="active"),
    ]

    Model = build_dynamic_model("UserModel", fields)

    instance = Model(
        name="Jane",
        age=28,
        rating=4.5,
        active=True,
        birthdate=today,
        joined_at=now,
        preferences={"theme": "dark"},
        tags=["dev", "test"],
        email="example@test.com",
        user_id=uid,
        status="new",
        profile={"first_name": "John", "last_name": "Doe", "contact_email": "john.doe@example.com"},
    )

    schema = Model.model_json_schema()
    print(schema)
    field_label = "name"
    assert schema["properties"][field_label]["description"] == "The user's given name"
    assert instance.name == "Jane"
    assert instance.status.name == "NEW"
    assert instance.status.value == "new"
    assert instance.profile.first_name == "John"
    assert instance.profile.last_name == "Doe"
    assert instance.profile.contact_email == "john.doe@example.com"


def test_build_from_function_basic_types_and_defaults():
    def create_user(
        name: str,
        age: int,
        email: str,
        rating: float = 4.5,
        active: bool = True,
        birthdate: date | None = None,
        joined_at: datetime | None = None,
        user_id: uuid.UUID = uuid.uuid4(),
        status: Literal["new", "active", "disabled"] = "active",
        tags: list[str] | None = None,
        profile: dict | None = None,
    ): ...

    Model = build_model_from_function(create_user)

    inst = Model(
        name="Jane",
        age=28,
        email="jane@example.com",
        rating=4.5,
        active=True,
        birthdate=date.today(),
        joined_at=datetime.utcnow(),
        user_id=uuid.uuid4(),
        status="active",
        tags=["dev", "test"],
        profile={"theme": "dark"},
    )

    # Basic assertions
    assert inst.name == "Jane"
    assert inst.age == 28
    assert inst.email == "jane@example.com"  # EmailField returns str in your tests
    assert inst.rating == 4.5
    assert inst.active is True
    assert inst.status.value == "active"  # Literal -> EnumField
    assert inst.tags == ["dev", "test"]
    assert inst.profile == {"theme": "dark"}

    # JSON schema exists and can be served to frontend
    schema = Model.model_json_schema()
    assert "properties" in schema


def test_overrides_constraints_and_description():
    def f(name: str, age: int, status: Literal["new", "active", "disabled"] = "active"): ...

    overrides = {
        "name": {"min_length": 2, "max_length": 10, "description": "The user's given name"},
        "age": {"ge_int": 0, "le_int": 120},
    }
    Model = build_model_from_function(f, overrides=overrides)

    ok = Model(name="Jane", age=30, status="active")
    assert ok.name == "Jane"
    assert ok.status.value == "active"

    # Constraint checks
    with pytest.raises(ValidationError):
        Model(name="J", age=30, status="active")
    with pytest.raises(ValidationError):
        Model(name="ThisIsWayTooLongName", age=30, status="active")
    with pytest.raises(ValidationError):
        Model(name="Jane", age=-1, status="active")
    with pytest.raises(ValidationError):
        Model(name="Jane", age=30, status="unknown")

    # Description propagated
    schema = Model.model_json_schema()
    assert schema["properties"]["name"]["description"] == "The user's given name"


def test_list_and_json_defaults_and_validation():
    def g(items: list[int], config: dict, email: str): ...

    Model = build_model_from_function(
        g,
        overrides={
            "items": {"default_list": [1, 2]},
            "config": {"default_dict": {"theme": "dark"}},
        },
    )

    ok = Model(items=[3, 4], config={"x": 1}, email="a@b.com")
    assert ok.items == [3, 4]
    assert ok.config == {"x": 1}
    assert ok.email == "a@b.com"

    with pytest.raises(ValidationError):
        Model(items="not-a-list", config={"x": 1}, email="a@b.com")

    with pytest.raises(ValidationError):
        Model(items=[1], config={"x": 1}, email="not-an-email")


def test_email_detection_vs_str_heuristic():
    def h(contact_email: str, label: str): ...

    Model = build_model_from_function(h)
    ok = Model(contact_email="u@example.com", label="foo")
    assert ok.contact_email == "u@example.com"
    assert ok.label == "foo"

    with pytest.raises(ValidationError):
        Model(contact_email="nope", label="foo")


def test_optional_types_are_allowed_when_none():
    def k(nickname: str | None = None, last_seen: datetime | None = None): ...

    Model = build_model_from_function(k)
    ok = Model()  # both optional
    assert ok.nickname is None
    assert ok.last_seen is None

    ok2 = Model(nickname="JJ", last_seen=datetime.utcnow())
    assert ok2.nickname == "JJ"


def test_nested_object_override():
    def q(profile: dict, email: str): ...

    profile_field = ObjectField(
        label="profile",
        fields=[
            StrField(label="first_name", default_str="John"),
            StrField(label="last_name", default_str="Doe"),
            EmailField(label="contact_email", default_email="john.doe@example.com"),
        ],
    )

    Model = build_model_from_function(
        q,
        overrides={
            "profile": {"field": profile_field},
        },
    )

    inst = Model(
        profile={"first_name": "Jane", "last_name": "Roe", "contact_email": "jane.roe@example.com"},
        email="x@y.com",
    )

    assert inst.profile.first_name == "Jane"
    assert inst.profile.last_name == "Roe"
    assert inst.profile.contact_email == "jane.roe@example.com"
    assert inst.email == "x@y.com"


def test_literal_enum_respects_default_and_allowed_values():
    def f(status: Literal["new", "active", "disabled"] = "active"): ...

    Model = build_model_from_function(f)
    ok = Model(status="new")
    assert ok.status.value == "new"

    # default if omitted
    ok2 = Model()
    assert ok2.status.value == "active"

    # invalid option
    with pytest.raises(ValidationError):
        Model(status="unknown")


def test_pep604_optional_unwraps_and_allows_none():
    def f(nickname: str | None = None, last_seen: datetime | None = None): ...

    Model = build_model_from_function(f)
    ok = Model()  # both omitted → None
    assert ok.nickname is None and ok.last_seen is None

    ok2 = Model(nickname="JJ", last_seen=datetime.utcnow())
    assert ok2.nickname == "JJ"


def test_list_and_dict_defaults_via_overrides_and_validation():
    def f(items: list[int], cfg: dict): ...

    Model = build_model_from_function(
        f,
        overrides={
            "items": {"default_list": [1, 2]},
            "cfg": {"default_dict": {"theme": "dark"}},
        },
    )

    # use caller-provided values
    ok = Model(items=[3, 4], cfg={"x": 1})
    assert ok.items == [3, 4] and ok.cfg == {"x": 1}

    # type errors
    with pytest.raises(ValidationError):
        Model(items="nope", cfg={"x": 1})
    with pytest.raises(ValidationError):
        Model(items=[3, 4], cfg="nope")


def test_email_heuristic_vs_plain_str():
    def f(work_email: str, label: str): ...

    Model = build_model_from_function(f)
    ok = Model(work_email="u@example.com", label="tag")
    assert ok.work_email == "u@example.com" and ok.label == "tag"

    with pytest.raises(ValidationError):
        Model(work_email="not-an-email", label="x")


def test_uuid_date_datetime_roundtrip_and_validation():
    def f(u: uuid.UUID, bday: date, seen: datetime): ...

    Model = build_model_from_function(f)

    good = Model(u=uuid.uuid4(), bday=date.today(), seen=datetime.utcnow())
    assert isinstance(good.u, uuid.UUID)
    assert isinstance(good.bday, date)
    assert isinstance(good.seen, datetime)

    # simple negative checks
    with pytest.raises(ValidationError):
        Model(u="not-uuid", bday=date.today(), seen=datetime.utcnow())
    with pytest.raises(ValidationError):
        Model(u=uuid.uuid4(), bday="2020-01-01", seen=datetime.utcnow())


def test_object_like_inference_with_annotations_attr():
    class Profile:
        __annotations__ = {
            "first": str,
            "last": str,
        }

    def f(profile: Profile, when: datetime): ...

    Model = build_model_from_function(f)
    m = Model(profile={"first": "Ada", "last": "Lovelace"}, when=datetime.utcnow())
    assert m.profile.first == "Ada"
    assert m.profile.last == "Lovelace"


def test_full_field_override_injects_custom_object_field():
    def f(profile: dict, email: str): ...

    profile_field = ObjectField(
        label="profile",
        fields=[
            StrField(label="first_name"),
            StrField(label="last_name"),
            EmailField(label="contact_email"),
        ],
    )

    Model = build_model_from_function(
        f,
        overrides={"profile": {"field": profile_field}},
    )

    ok = Model(profile={"first_name": "Jane", "last_name": "Roe", "contact_email": "jane@x.com"}, email="x@y.com")
    assert ok.profile.first_name == "Jane"
    assert ok.profile.last_name == "Roe"
    assert ok.profile.contact_email == "jane@x.com"
    assert ok.email == "x@y.com"

    # invalid nested email should fail
    with pytest.raises(ValidationError):
        Model(profile={"first_name": "A", "last_name": "B", "contact_email": "nope"}, email="x@y.com")


def test_override_constraints_merge_and_description_propagates():
    def f(name: str, age: int): ...

    overrides = {
        "name": {"min_length": 2, "max_length": 6, "description": "Display name"},
        "age": {"ge_int": 0, "le_int": 120},
    }
    Model = build_model_from_function(f, overrides=overrides)

    ok = Model(name="Alice", age=30)
    assert ok.name == "Alice"

    with pytest.raises(ValidationError):
        Model(name="A", age=30)
    with pytest.raises(ValidationError):
        Model(name="TooLongName", age=30)
    with pytest.raises(ValidationError):
        Model(name="Alice", age=-1)

    schema = Model.model_json_schema()
    assert schema["properties"]["name"]["description"] == "Display name"


def test_json_fallback_rejects_non_dict_inputs():
    # Unknown/complex type → JsonField (expects dict input at runtime)
    class Weird:
        pass

    def f(payload: Weird): ...

    Model = build_model_from_function(f)

    with pytest.raises(ValidationError):
        Model(payload="string-not-dict")

    ok = Model(payload={"x": 1})
    assert ok.payload == {"x": 1}


def test_defaults_from_signature_are_used_when_missing():
    def f(
        rating: float = 4.5,
        active: bool = True,
        since: datetime = datetime.utcnow() - timedelta(days=1),
        tags: list[str] | None = None,
        profile: dict | None = None,
    ): ...

    Model = build_model_from_function(f)

    # supply nothing → defaults applied
    inst = Model()
    # Can't assert the exact datetime, but ensure it's datetime and in the past (roughly)
    assert isinstance(inst.since, datetime)
    assert inst.active is True
    assert inst.rating == 4.5
