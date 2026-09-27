import pytest

from rediskit.utils import has_glob_pattern


@pytest.mark.parametrize(
    "value, expected",
    [
        ("redis_kit_node:tenant:metrics:key", False),
        ("plain-key_with.dots", False),
        ("", False),
        ("redis_kit_node:*:tenant", True),
        ("prefix:key*", True),
        ("key?", True),
        ("key[ab]", True),
        ("key\\*", True),  # escaped metachar: keep MATCH semantics, still a pattern
    ],
)
def test_has_glob_pattern(value, expected):
    assert has_glob_pattern(value) is expected
