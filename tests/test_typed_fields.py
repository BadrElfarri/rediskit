import uuid
from datetime import date, datetime

import pytest
from pydantic import ValidationError

from rediskit.typed_fields import ObjectField
from rediskit.typed_fields.base_field import build_dynamic_model
from rediskit.typed_fields.bool_field import BoolField
from rediskit.typed_fields.date_field import DateField, DateTimeField
from rediskit.typed_fields.email_field import EmailField
from rediskit.typed_fields.enum_field import EnumField
from rediskit.typed_fields.float_field import FloatField
from rediskit.typed_fields.int_field import IntField
from rediskit.typed_fields.json_field import JsonField
from rediskit.typed_fields.list_field import ListField
from rediskit.typed_fields.str_field import StrField
from rediskit.typed_fields.uuid_field import UuidField


def test_str_field_constraints():
    field = StrField(label="name", min_length=2, max_length=5, value="abc")
    model_cls = build_dynamic_model("StrModel", [field])
    obj = model_cls(name="test")
    assert obj.name == "test"

    with pytest.raises(ValidationError):
        model_cls(name="a")
    with pytest.raises(ValidationError):
        model_cls(name="toolong")


def test_int_field_bounds():
    field = IntField(label="age", ge=18, le=65, value=30)
    model_cls = build_dynamic_model("IntModel", [field])
    obj = model_cls(age=25)
    assert obj.age == 25

    with pytest.raises(ValidationError):
        model_cls(age=10)
    with pytest.raises(ValidationError):
        model_cls(age=100)


def test_float_field_bounds():
    field = FloatField(label="score", ge=0.0, le=1.0, value=0.5)
    model_cls = build_dynamic_model("FloatModel", [field])
    obj = model_cls(score=0.75)
    assert obj.score == 0.75

    with pytest.raises(ValidationError):
        model_cls(score=1.5)


def test_boolean_field():
    field = BoolField(label="active", value=True)
    model_cls = build_dynamic_model("BoolModel", [field])
    obj = model_cls(active=False)
    assert obj.active is False


def test_date_field():
    today = date.today()
    field = DateField(label="created", value=today)
    model_cls = build_dynamic_model("DateModel", [field])
    obj = model_cls(created=today)
    assert obj.created == today


def test_datetime_field():
    now = datetime.utcnow()
    field = DateTimeField(label="timestamp", value=now)
    model_cls = build_dynamic_model("DateTimeModel", [field])
    obj = model_cls(timestamp=now)
    assert obj.timestamp == now


def test_json_field():
    value = {"key": "value"}
    field = JsonField(label="config", value=value)
    model_cls = build_dynamic_model("JsonModel", [field])
    obj = model_cls(config=value)
    assert obj.config == value


def test_list_field():
    value = [1, 2, 3]
    field = ListField(label="items", value=value)
    model_cls = build_dynamic_model("ListModel", [field])
    obj = model_cls(items=value)
    assert obj.items == value


def test_email_field():
    email = "user@example.com"
    field = EmailField(label="email", value=email)
    model_cls = build_dynamic_model("EmailModel", [field])
    obj = model_cls(email=email)
    assert isinstance(obj.email, str)

    with pytest.raises(ValidationError):
        model_cls(email="not-an-email")


def test_uuid_field():
    uid = uuid.uuid4()
    field = UuidField(label="identifier", value=uid)
    model_cls = build_dynamic_model("UuidModel", [field])
    obj = model_cls(identifier=uid)
    assert obj.identifier == uid

    with pytest.raises(ValidationError):
        model_cls(identifier="invalid-uuid")


def test_enum_field():
    options = ["red", "green", "blue"]
    field = EnumField(label="color", allowed_values=options, value="red")
    model_cls = build_dynamic_model("EnumModel", [field])
    obj = model_cls(color="green")
    assert obj.color.value == "green"

    with pytest.raises(ValidationError):
        model_cls(color="yellow")


def test_nested_object_field():
    inner_fields = [
        StrField(label="first_name", min_length=1, max_length=50, value="John"),
        StrField(label="last_name", min_length=1, max_length=50, value="Doe"),
        EmailField(label="contact_email", value="john.doe@example.com"),
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
            value="Jane",
            description="The user's given name",
        ),
        IntField(label="age", ge=0, le=120, value=28),
        FloatField(label="rating", ge=0.0, le=5.0, value=4.5),
        BoolField(label="active", value=True),
        DateField(label="birthdate", value=today),
        DateTimeField(label="joined_at", value=now),
        JsonField(label="preferences", value={"theme": "dark"}),
        ListField(label="tags", value=["dev", "test"]),
        EmailField(label="email", value="example@test.com"),
        UuidField(label="user_id", value=uid),
        EnumField(label="status", allowed_values=["new", "active", "disabled"], value="active"),
        ObjectField(
            label="profile",
            fields=[
                StrField(label="first_name", value="John"),
                StrField(label="last_name", value="Doe"),
                EmailField(label="contact_email", value="john.doe@example.com"),
            ],
        ),
        EmailField(label="email", value="example@test.com"),
        UuidField(label="user_id", value=uid),
        EnumField(label="status", allowed_values=["new", "active", "disabled"], value="active"),
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
