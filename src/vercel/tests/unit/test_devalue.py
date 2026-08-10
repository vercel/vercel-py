"""Tests for vercel._internal.devalue — Python port of JS devalue.

Test cases are modelled after the JS test suite at
https://github.com/sveltejs/devalue/blob/main/test/index.test.js
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone

import pytest

from vercel._internal.devalue import (
    DevalueError,
    Hole,
    JsBigInt,
    JsRegExp,
    Undefined,
    parse,
    stringify,
    unflatten,
)
from vercel._internal.devalue.constants import (
    HOLE,
    MAX_ARRAY_LEN,
    MAX_SAFE_INTEGER,
    MAX_SPARSE_ARRAY_LENGTH,
    NAN,
    NEGATIVE_INFINITY,
    NEGATIVE_ZERO,
    POSITIVE_INFINITY,
    SPARSE,
    UNDEFINED,
)

# ═══════════════════════════════════════════════════════════════════════════
# stringify – primitives
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyPrimitives:
    def test_positive_integer(self):
        assert stringify(42) == "[42]"

    def test_negative_integer(self):
        assert stringify(-5) == "[-5]"

    def test_positive_decimal(self):
        assert stringify(0.1) == "[0.1]"

    def test_negative_decimal(self):
        assert stringify(-0.1) == "[-0.1]"

    def test_nan(self):
        assert stringify(float("nan")) == str(NAN)

    def test_positive_infinity(self):
        assert stringify(float("inf")) == str(POSITIVE_INFINITY)

    def test_negative_infinity(self):
        assert stringify(float("-inf")) == str(NEGATIVE_INFINITY)

    def test_zero(self):
        assert stringify(0) == "[0]"

    def test_negative_zero(self):
        assert stringify(-0.0) == str(NEGATIVE_ZERO)

    def test_string(self):
        assert stringify("woo!!!") == '["woo!!!"]'

    def test_boolean_true(self):
        assert stringify(True) == "[true]"

    def test_boolean_false(self):
        assert stringify(False) == "[false]"

    def test_undefined(self):
        assert stringify(Undefined) == str(UNDEFINED)

    def test_none(self):
        assert stringify(None) == "[null]"


# ═══════════════════════════════════════════════════════════════════════════
# stringify – basic complex types
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyBasics:
    def test_regex_with_flags(self):
        p = re.compile("regexp", re.IGNORECASE | re.MULTILINE)
        result = stringify(p)
        assert result == '[["RegExp","regexp","im"]]'

    def test_regex_no_flags(self):
        p = re.compile("regexp")
        result = stringify(p)
        assert result == '[["RegExp","regexp"]]'

    def test_date(self):
        dt = datetime(2001, 9, 9, 1, 46, 40, tzinfo=timezone.utc)
        assert stringify(dt) == '[["Date","2001-09-09T01:46:40.000Z"]]'

    def test_list(self):
        assert stringify(["a", "b", "c"]) == '[[1,2,3],"a","b","c"]'

    def test_empty_list(self):
        assert stringify([]) == "[[]]"

    def test_dict(self):
        assert stringify({"foo": "bar", "x-y": "z"}) == '[{"foo":1,"x-y":2},"bar","z"]'

    def test_empty_dict(self):
        assert stringify({}) == "[{}]"

    def test_set(self):
        result = stringify({1, 2, 3})
        parsed = parse(result)
        assert parsed == {1, 2, 3}

    def test_bytes(self):
        # Byte-identical to `devalue.stringify(new Uint8Array([1, 2, 3]))`.
        assert stringify(b"\x01\x02\x03") == '[["Uint8Array",1],["ArrayBuffer","AQID"]]'

    def test_bytearray(self):
        assert stringify(bytearray(b"\x01\x02\x03")) == '[["ArrayBuffer","AQID"]]'

    def test_memoryview(self):
        assert stringify(memoryview(b"\x01\x02\x03")) == '[["ArrayBuffer","AQID"]]'

    def test_tuple_as_array(self):
        assert stringify(("a", "b")) == '[[1,2],"a","b"]'

    def test_nested_structure(self):
        val = {"list": [1, 2], "flag": True}
        result = stringify(val)
        assert parse(result) == val


# ═══════════════════════════════════════════════════════════════════════════
# stringify – strings (escaping)
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyStrings:
    def test_newline(self):
        assert stringify("a\nb") == '["a\\nb"]'

    def test_double_quotes(self):
        assert stringify('"yar"') == '["\\"yar\\""]'

    def test_nul(self):
        assert stringify("\0") == '["\\u0000"]'

    def test_control_character(self):
        assert stringify("\u0001") == '["\\u0001"]'

    def test_control_character_extremum(self):
        assert stringify("\u001f") == '["\\u001f"]'

    def test_backslash(self):
        assert stringify("\\") == '["\\\\"]'

    def test_tab(self):
        assert stringify("\t") == '["\\t"]'

    def test_carriage_return(self):
        assert stringify("\r") == '["\\r"]'

    def test_unicode_2028(self):
        assert stringify("\u2028") == '["\\u2028"]'

    def test_unicode_2029(self):
        assert stringify("\u2029") == '["\\u2029"]'


# ═══════════════════════════════════════════════════════════════════════════
# stringify – XSS prevention
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyXSS:
    def test_dangerous_string(self):
        value = "</script><script src='https://evil.com/script.js'>alert('pwned')</script><script>"
        result = stringify(value)
        assert "\\u003C" in result
        assert "<script" not in result

    def test_dangerous_key(self):
        value = {'<svg onload=alert("xss_works")>': "bar"}
        result = stringify(value)
        assert "\\u003C" in result
        assert "<svg" not in result

    def test_dangerous_regex(self):
        pattern = re.compile("[</script><script>alert('xss')//]")
        result = stringify(pattern)
        assert "\\u003C" in result
        assert "<script" not in result


# ═══════════════════════════════════════════════════════════════════════════
# stringify – cycles
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyCycles:
    def test_dict_cycle(self):
        obj: dict = {}
        obj["self"] = obj
        result = stringify(obj)
        assert result == '[{"self":0}]'

    def test_list_cycle(self):
        arr: list = [None]
        arr[0] = arr
        result = stringify(arr)
        assert result == "[[0]]"

    def test_mutual_cycle(self):
        first: dict = {}
        second: dict = {}
        first["second"] = second
        second["first"] = first
        result = stringify([first, second])
        assert result == '[[1,2],{"second":2},{"first":1}]'


# ═══════════════════════════════════════════════════════════════════════════
# stringify – repetition / deduplication
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyRepetition:
    def test_string_repetition(self):
        assert stringify(["a string", "a string"]) == '[[1,1],"a string"]'

    def test_none_repetition(self):
        assert stringify([None, None]) == "[[1,1],null]"

    def test_nan_repetition(self):
        assert stringify([float("nan"), float("nan")]) == f"[[{NAN},{NAN}]]"

    def test_dict_repetition(self):
        obj: dict = {}
        assert stringify([obj, obj]) == "[[1,1],{}]"

    def test_regex_repetition(self):
        p = re.compile("regexp")
        result = stringify([p, p])
        assert result == '[[1,1],["RegExp","regexp"]]'

    def test_date_repetition(self):
        dt = datetime(2001, 9, 9, 1, 46, 40, tzinfo=timezone.utc)
        result = stringify([dt, dt])
        assert result == '[[1,1],["Date","2001-09-09T01:46:40.000Z"]]'

    def test_bytes_repetition(self):
        # One view and one buffer, however many times the value appears.
        buf = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
        result = stringify([buf, buf])
        assert result == '[[1,1],["Uint8Array",2],["ArrayBuffer","AAECAwQFBgcICQ=="]]'


# ═══════════════════════════════════════════════════════════════════════════
# stringify – custom reducers
# ═══════════════════════════════════════════════════════════════════════════


class Foo:
    def __init__(self, value):
        self.value = value


class Bar:
    def __init__(self, value):
        self.value = value


class TestStringifyCustom:
    def test_custom_type(self):
        instance = Foo({"bar": Bar({"answer": 42})})
        reducers = {
            "Foo": lambda x: x.value if isinstance(x, Foo) else None,
            "Bar": lambda x: x.value if isinstance(x, Bar) else None,
        }
        result = stringify([instance, instance], reducers)
        assert result == '[[1,1],["Foo",2],{"bar":3},["Bar",4],{"answer":5},42]'

    def test_custom_round_trip(self):
        instance = Foo({"bar": Bar({"answer": 42})})
        reducers = {
            "Foo": lambda x: x.value if isinstance(x, Foo) else None,
            "Bar": lambda x: x.value if isinstance(x, Bar) else None,
        }
        revivers = {
            "Foo": lambda x: Foo(x),
            "Bar": lambda x: Bar(x),
        }
        result = parse(stringify([instance, instance], reducers), revivers)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] is result[1]
        assert isinstance(result[0], Foo)
        assert isinstance(result[0].value["bar"], Bar)
        assert result[0].value["bar"].value["answer"] == 42


# ═══════════════════════════════════════════════════════════════════════════
# stringify – errors
# ═══════════════════════════════════════════════════════════════════════════


class TestStringifyErrors:
    def test_function_raises(self):
        with pytest.raises(DevalueError, match="Cannot stringify a function"):
            stringify(lambda x: x)

    def test_non_string_dict_key_raises(self):
        with pytest.raises(DevalueError, match="non-string keys"):
            stringify({1: "value"})

    def test_proto_key_raises(self):
        obj = json.loads('{"__proto__": 1}')
        root = {"foo": obj}
        with pytest.raises(DevalueError, match="__proto__") as exc_info:
            stringify(root)
        assert exc_info.value.path == ".foo"
        assert exc_info.value.name == "DevalueError"

    def test_non_pojo_raises(self):
        class Custom:
            pass

        with pytest.raises(DevalueError, match="non-POJOs"):
            stringify(Custom())

    def test_error_path_nested(self):
        with pytest.raises(DevalueError) as exc_info:
            stringify({"foo": {"array": [lambda: None]}})
        assert exc_info.value.path == ".foo.array[0]"

    def test_error_path_map_then_object(self):
        with pytest.raises(DevalueError) as exc_info:
            stringify({"aset": {1, 2}, "object": {"invalid": lambda: None}})
        assert exc_info.value.path == ".object.invalid"

    def test_error_value_attribute(self):
        fn = lambda: None  # noqa: E731
        with pytest.raises(DevalueError) as exc_info:
            stringify({"foo": {"array": [fn]}})
        assert exc_info.value.value is fn

    def test_error_root_attribute(self):
        root = {"foo": {"array": [lambda: None]}}
        with pytest.raises(DevalueError) as exc_info:
            stringify(root)
        assert exc_info.value.root is root


# ═══════════════════════════════════════════════════════════════════════════
# parse – primitives
# ═══════════════════════════════════════════════════════════════════════════


class TestParsePrimitives:
    def test_positive_integer(self):
        assert parse("[42]") == 42

    def test_negative_integer(self):
        assert parse("[-5]") == -5

    def test_positive_decimal(self):
        assert parse("[0.1]") == pytest.approx(0.1)

    def test_negative_decimal(self):
        assert parse("[-0.1]") == pytest.approx(-0.1)

    def test_nan(self):
        assert math.isnan(parse(str(NAN)))

    def test_positive_infinity(self):
        assert parse(str(POSITIVE_INFINITY)) == float("inf")

    def test_negative_infinity(self):
        assert parse(str(NEGATIVE_INFINITY)) == float("-inf")

    def test_zero(self):
        assert parse("[0]") == 0

    def test_negative_zero(self):
        result = parse(str(NEGATIVE_ZERO))
        assert result == 0.0
        assert math.copysign(1, result) < 0

    def test_string(self):
        assert parse('["woo!!!"]') == "woo!!!"

    def test_boolean(self):
        assert parse("[true]") is True

    def test_undefined(self):
        assert parse(str(UNDEFINED)) is Undefined

    def test_null(self):
        assert parse("[null]") is None

    def test_bigint(self):
        assert parse('[["BigInt","12345678901234567890"]]') == 12345678901234567890


# ═══════════════════════════════════════════════════════════════════════════
# parse – basic complex types
# ═══════════════════════════════════════════════════════════════════════════


class TestParseBasics:
    def test_date(self):
        result = parse('[["Date","2001-09-09T01:46:40.000Z"]]')
        assert isinstance(result, datetime)
        assert result == datetime(2001, 9, 9, 1, 46, 40, tzinfo=timezone.utc)

    def test_invalid_date(self):
        result = parse('[["Date",""]]')
        assert result is None

    def test_regex(self):
        result = parse('[["RegExp","regexp","gim"]]')
        assert result == JsRegExp("regexp", "gim")
        # `g` has no Python counterpart but survives the round trip.
        assert result.flags == "gim"
        assert result.compile().flags & re.IGNORECASE

    def test_regex_no_flags(self):
        result = parse('[["RegExp","test"]]')
        assert result == JsRegExp("test", "")

    def test_array(self):
        assert parse('[[1,2,3],"a","b","c"]') == ["a", "b", "c"]

    def test_empty_array(self):
        assert parse("[[]]") == []

    def test_sparse_array(self):
        json_str = f'[[{HOLE},1,{HOLE}],"b"]'
        result = parse(json_str)
        assert len(result) == 3
        assert result[0] is Hole
        assert result[1] == "b"
        assert result[2] is Hole

    def test_very_sparse_array(self):
        json_str = f'[[{SPARSE},1000001,1000000,1],"x"]'
        result = parse(json_str)
        assert len(result) == 1000001
        assert result[1000000] == "x"
        assert result[0] is Hole

    def test_object(self):
        assert parse('[{"foo":1,"x-y":2},"bar","z"]') == {
            "foo": "bar",
            "x-y": "z",
        }

    def test_set(self):
        assert parse('[["Set",1,2,3],1,2,3]') == {1, 2, 3}

    def test_map_as_dict(self):
        assert parse('[["Map",1,2],"a","b"]') == {"a": "b"}

    def test_arraybuffer(self):
        result = parse('[["ArrayBuffer","AQID"]]')
        assert result == b"\x01\x02\x03"
        assert type(result) is bytearray

    def test_typed_array(self):
        result = parse('[["Uint8Array",1],["ArrayBuffer","AQID"]]')
        assert result == b"\x01\x02\x03"
        assert type(result) is bytes

    def test_typed_array_does_not_alias_its_buffer(self):
        view, buffer = parse('[[1,2],["Uint8Array",2],["ArrayBuffer","AQID"]]')
        buffer[0] = 0xFF
        assert view == b"\x01\x02\x03"

    def test_url_as_string(self):
        result = parse('[["URL","https://example.com/path"]]')
        assert result == "https://example.com/path"

    def test_url_search_params_as_string(self):
        result = parse('[["URLSearchParams","foo=1&bar=2"]]')
        assert result == "foo=1&bar=2"

    def test_temporal_as_string(self):
        result = parse('[["Temporal.Duration","P1Y2M3D"]]')
        assert result == "P1Y2M3D"

    def test_null_prototype_object(self):
        result = parse('[["null"]]')
        assert result == {}

    def test_null_prototype_object_with_keys(self):
        result = parse('[["null","key",1],"value"]')
        assert result == {"key": "value"}

    def test_object_wrapper_returns_inner_value(self):
        # ["Object", idx] → unwrap to the inner value
        assert parse('[["Object",1],42]') == 42

    def test_object_wrapper_bigint(self):
        assert parse('[["Object",1],["BigInt","1"]]') == 1

    def test_sliced_typed_array(self):
        # Uint16Array with byteOffset=2, length=2 from an 8-byte buffer
        result = parse('[["Uint16Array",1,2,2],["ArrayBuffer","CgAUAB4AKAA="]]')
        assert isinstance(result, bytes)
        assert len(result) == 4  # 2 elements × 2 bytes each


# ═══════════════════════════════════════════════════════════════════════════
# parse – cycles
# ═══════════════════════════════════════════════════════════════════════════


class TestParseCycles:
    def test_dict_cycle(self):
        result = parse('[{"self":0}]')
        assert result["self"] is result

    def test_list_cycle(self):
        result = parse("[[0]]")
        assert result[0] is result

    def test_mutual_cycle(self):
        result = parse('[[1,2],{"second":2},{"first":1}]')
        assert result[0]["second"] is result[1]
        assert result[1]["first"] is result[0]

    def test_map_cycle(self):
        result = parse('[["Map",1,0],"self"]')
        assert isinstance(result, dict)
        assert result["self"] is result

    def test_null_proto_cycle(self):
        result = parse('[["null","self",0]]')
        assert result["self"] is result


# ═══════════════════════════════════════════════════════════════════════════
# parse – repetition / deduplication
# ═══════════════════════════════════════════════════════════════════════════


class TestParseRepetition:
    def test_string_repetition(self):
        result = parse('[[1,1],"a string"]')
        assert result == ["a string", "a string"]

    def test_null_repetition(self):
        result = parse("[[1,1],null]")
        assert result == [None, None]

    def test_nan_repetition(self):
        result = parse(f"[[{NAN},{NAN}]]")
        assert len(result) == 2
        assert all(math.isnan(x) for x in result)

    def test_object_repetition(self):
        result = parse("[[1,1],{}]")
        assert result[0] is result[1]

    def test_regex_repetition(self):
        result = parse('[[1,1],["RegExp","regexp"]]')
        assert result[0] is result[1]
        assert isinstance(result[0], JsRegExp)

    def test_date_repetition(self):
        result = parse('[[1,1],["Date","2001-09-09T01:46:40.000Z"]]')
        assert result[0] is result[1]
        assert isinstance(result[0], datetime)


# ═══════════════════════════════════════════════════════════════════════════
# parse – custom revivers
# ═══════════════════════════════════════════════════════════════════════════


class TestParseCustom:
    def test_custom_reviver(self):
        revivers = {
            "Foo": lambda x: Foo(x),
            "Bar": lambda x: Bar(x),
        }
        result = parse(
            '[[1,1],["Foo",2],{"bar":3},["Bar",4],{"answer":5},42]',
            revivers,
        )
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] is result[1]
        assert isinstance(result[0], Foo)
        assert isinstance(result[0].value["bar"], Bar)
        assert result[0].value["bar"].value["answer"] == 42


# ═══════════════════════════════════════════════════════════════════════════
# parse – error cases
# ═══════════════════════════════════════════════════════════════════════════


class TestParseErrors:
    def test_empty_string(self):
        with pytest.raises(json.JSONDecodeError):
            parse("")

    def test_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            parse("][")

    def test_hole_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(str(HOLE))

    def test_string_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse('"hello"')

    def test_number_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse("42")

    def test_boolean_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse("true")

    def test_null_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse("null")

    def test_object_standalone(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse("{}")

    def test_empty_array(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse("[]")

    def test_prototype_pollution(self):
        with pytest.raises(ValueError, match="__proto__"):
            parse('[{"__proto__":1},{}]')

    def test_prototype_pollution_null_proto(self):
        with pytest.raises(ValueError, match="__proto__"):
            parse('[["null","__proto__",1],{}]')

    def test_nested_prototype_pollution_null_proto(self):
        with pytest.raises(ValueError, match="__proto__"):
            parse('[{"data":1},["null","__proto__",2],{"polluted":3},true]')

    def test_prototype_pollution_via_object_wrapper(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse('[["Object",{"__proto__":1}],{}]')

    def test_typed_array_with_non_arraybuffer(self):
        with pytest.raises(ValueError, match="Invalid data"):
            parse('[["Int8Array", 1], { "length": 2 }, 1000000000]')

    def test_arraybuffer_with_non_string(self):
        with pytest.raises(ValueError, match="Invalid ArrayBuffer encoding"):
            parse('[["ArrayBuffer", { "length": 100 }]]')

    def test_sparse_array_proto_pollution(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 1, "__proto__", {"polluted": True}]]))

    def test_sparse_array_non_integer_index(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 5, "foo", 1]]))

    def test_sparse_array_negative_index(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 5, -1, 1]]))

    def test_sparse_array_out_of_bounds(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 2, 5, 1]]))

    def test_sparse_array_non_integer_length(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, "abc"]]))

    def test_sparse_array_negative_length(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, -3]]))

    def test_sparse_array_float_length(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 1.5]]))

    def test_sparse_array_float_index(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, 5, 1.5, 1]]))

    def test_sparse_array_length_above_max_array_len(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(json.dumps([[SPARSE, MAX_ARRAY_LEN + 1]]))

    def test_typed_array_self_reference(self):
        with pytest.raises(ValueError, match="Invalid data"):
            parse('[["Uint8Array", 0]]')

    def test_custom_reviver_self_reference(self):
        with pytest.raises(ValueError, match="Invalid circular reference"):
            parse('[["Custom", 0]]', revivers={"Custom": lambda v: v})

    def test_mutual_typed_array_reference(self):
        with pytest.raises(ValueError, match="Invalid data"):
            parse('[["Uint8Array", 1], ["Uint8Array", 0]]')

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown type"):
            parse('[["NoSuchType","data"]]')

    def test_bad_index(self):
        with pytest.raises(ValueError, match="Invalid input"):
            parse('[{"0":1,"toString":"push"},"hello"]')


# ═══════════════════════════════════════════════════════════════════════════
# unflatten (direct, without JSON.parse step)
# ═══════════════════════════════════════════════════════════════════════════


class TestUnflatten:
    def test_special_constants(self):
        assert unflatten(UNDEFINED) is Undefined
        assert math.isnan(unflatten(NAN))
        assert unflatten(POSITIVE_INFINITY) == float("inf")
        assert unflatten(NEGATIVE_INFINITY) == float("-inf")
        result = unflatten(NEGATIVE_ZERO)
        assert result == 0.0 and math.copysign(1, result) < 0

    def test_array(self):
        assert unflatten([[1, 2, 3], "a", "b", "c"]) == ["a", "b", "c"]

    def test_object(self):
        assert unflatten([{"foo": 1}, "bar"]) == {"foo": "bar"}

    def test_rejects_standalone_number(self):
        with pytest.raises(ValueError, match="Invalid input"):
            unflatten(42)

    def test_rejects_empty_list(self):
        with pytest.raises(ValueError, match="Invalid input"):
            unflatten([])

    def test_rejects_non_list(self):
        with pytest.raises(ValueError, match="Invalid input"):
            unflatten("hello")

    def test_sparse_proto_pollution_unflatten(self):
        with pytest.raises(ValueError, match="Invalid input"):
            unflatten([[SPARSE, 1, "__proto__", {"polluted": True}]])


# ═══════════════════════════════════════════════════════════════════════════
# round-trip tests (stringify → parse)
# ═══════════════════════════════════════════════════════════════════════════


class TestRoundTrip:
    """Verify that stringify → parse produces an equivalent value."""

    def test_integer(self):
        assert parse(stringify(42)) == 42

    def test_negative_integer(self):
        assert parse(stringify(-5)) == -5

    def test_float(self):
        assert parse(stringify(0.1)) == pytest.approx(0.1)

    def test_nan(self):
        assert math.isnan(parse(stringify(float("nan"))))

    def test_infinity(self):
        assert parse(stringify(float("inf"))) == float("inf")

    def test_neg_infinity(self):
        assert parse(stringify(float("-inf"))) == float("-inf")

    def test_neg_zero(self):
        result = parse(stringify(-0.0))
        assert result == 0.0
        assert math.copysign(1, result) < 0

    def test_string(self):
        assert parse(stringify("hello world")) == "hello world"

    def test_none(self):
        assert parse(stringify(None)) is None

    def test_true(self):
        assert parse(stringify(True)) is True

    def test_false(self):
        assert parse(stringify(False)) is False

    def test_undefined(self):
        assert parse(stringify(Undefined)) is Undefined

    def test_list(self):
        assert parse(stringify([1, "two", None])) == [1, "two", None]

    def test_dict(self):
        assert parse(stringify({"a": 1, "b": "c"})) == {"a": 1, "b": "c"}

    def test_set(self):
        assert parse(stringify({1, 2, 3})) == {1, 2, 3}

    def test_date(self):
        dt = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
        result = parse(stringify(dt))
        assert isinstance(result, datetime)
        assert result == dt

    def test_regex(self):
        # A Python pattern is accepted on the way out but lands as the
        # lossless container, not as a `re.Pattern` again.
        p = re.compile("foo.*bar", re.IGNORECASE)
        result = parse(stringify(p))
        assert result == JsRegExp("foo.*bar", "i")
        assert result.compile().flags & re.IGNORECASE

    def test_bytes(self):
        data = b"\x00\x01\x02\xff"
        result = parse(stringify(data))
        assert result == data
        assert type(result) is bytes

    def test_bytearray(self):
        data = bytearray(b"\x00\x01\x02\xff")
        result = parse(stringify(data))
        assert result == data
        assert type(result) is bytearray

    def test_nested(self):
        val = {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ],
            "count": 2,
        }
        assert parse(stringify(val)) == val

    def test_cycle_dict(self):
        obj: dict = {"x": 1}
        obj["self"] = obj
        result = parse(stringify(obj))
        assert result["x"] == 1
        assert result["self"] is result

    def test_cycle_list(self):
        arr: list = [1, 2]
        arr.append(arr)
        result = parse(stringify(arr))
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] is result

    def test_shared_reference(self):
        inner = {"x": 1}
        outer = [inner, inner]
        result = parse(stringify(outer))
        assert result[0] is result[1]
        assert result[0] == {"x": 1}


# ═══════════════════════════════════════════════════════════════════════════
# valid sparse array
# ═══════════════════════════════════════════════════════════════════════════


class TestSparseArray:
    def test_valid_sparse_array(self):
        payload = json.dumps([[SPARSE, 3, 0, 1, 2, 2], "a", "c"])
        result = parse(payload)
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0] == "a"
        assert result[1] is Hole
        assert result[2] == "c"

    def test_very_sparse_multiple_values(self):
        payload = json.dumps([[SPARSE, 21, 10, 1, 20, 2], "a", "b"])
        result = parse(payload)
        assert len(result) == 21
        assert result[10] == "a"
        assert result[20] == "b"
        assert result[0] is Hole

    def test_array_with_negative_zero_after_zero(self):
        result = parse(f"[[1,{NEGATIVE_ZERO}],0]")
        assert result[0] == 0
        assert result[1] == 0.0
        assert math.copysign(1, result[1]) < 0


# ═══════════════════════════════════════════════════════════════════════════
# cross-format: verify Python can parse JSON produced by JS devalue
# ═══════════════════════════════════════════════════════════════════════════


class TestJSCompat:
    """Verify that JSON wire-format strings from JS devalue can be parsed."""

    def test_js_boxed_number(self):
        # JS: new Number(42) → '["Object",1],42]'
        assert parse('[["Object",1],42]') == 42

    def test_js_boxed_string(self):
        assert parse('[["Object",1],"woo!!!"]') == "woo!!!"

    def test_js_boxed_boolean(self):
        assert parse('[["Object",1],true]') is True

    def test_js_set(self):
        assert parse('[["Set",1,2,3],1,2,3]') == {1, 2, 3}

    def test_js_map(self):
        assert parse('[["Map",1,2],"a","b"]') == {"a": "b"}

    def test_js_url(self):
        url = "https://user:password@example.com/%3Cscript%3E/path?foo=bar#hash"
        assert parse(f'[["URL","{url}"]]') == url

    def test_js_url_search_params(self):
        assert parse('[["URLSearchParams","foo=1&foo=2&baz=%3C+%3E"]]') == "foo=1&foo=2&baz=%3C+%3E"

    def test_js_temporal_duration(self):
        assert parse('[["Temporal.Duration","P1Y2M3D"]]') == "P1Y2M3D"

    def test_js_temporal_instant(self):
        assert parse('[["Temporal.Instant","1999-09-29T05:30:00Z"]]') == "1999-09-29T05:30:00Z"

    def test_js_null_prototype_empty(self):
        assert parse('[["null"]]') == {}

    def test_js_null_prototype_with_data(self):
        result = parse('[{"foo":1,"self":0},"bar"]')
        assert result["foo"] == "bar"
        assert result["self"] is result


# ═══════════════════════════════════════════════════════════════════════════
# regressions — places where the port used to diverge from JS devalue
# ═══════════════════════════════════════════════════════════════════════════


def _caching_reviver(cls, cache):
    """Reviver that returns one instance per payload.

    Stands in for the ``WeakMap`` the JS suite uses; the payload is kept
    alive by the parser for the duration of the call, so ``id`` is stable.
    """

    def revive(payload):
        instance = cache.get(id(payload))
        if instance is None:
            instance = cls.__new__(cls)
            cache[id(payload)] = instance
        instance.value = payload
        return instance

    return revive


class TestCircularRevivers:
    """Ported from the JS suite's `circular references through custom types`.

    A payload that already finished hydrating must be revived directly rather
    than tripping the in-progress guard.
    """

    def test_resolves_circular_reference_through_two_custom_types(self):
        foo = Foo({"name": "outer"})
        bar = Bar({"name": "inner", "ref": foo})
        foo.value["ref"] = bar

        reducers = {
            "Foo": lambda x: x.value if isinstance(x, Foo) else None,
            "Bar": lambda x: x.value if isinstance(x, Bar) else None,
        }
        revivers = {
            "Foo": _caching_reviver(Foo, {}),
            "Bar": _caching_reviver(Bar, {}),
        }

        result = parse(stringify(foo, reducers), revivers)

        assert isinstance(result, Foo)
        assert isinstance(result.value["ref"], Bar)
        assert result.value["ref"].value["ref"] is result

    def test_resolves_self_referencing_custom_type(self):
        foo = Foo({"name": "self"})
        foo.value["ref"] = foo

        reducers = {"Foo": lambda x: x.value if isinstance(x, Foo) else None}
        revivers = {"Foo": _caching_reviver(Foo, {})}

        result = parse(stringify(foo, reducers), revivers)

        assert isinstance(result, Foo)
        assert result.value["ref"] is result

    def test_uncached_reviver_still_resolves_the_cycle(self):
        result = parse('[["C",1],{"self":0}]', {"C": lambda v: {"wrapped": v}})
        assert result["wrapped"]["self"]["wrapped"] is result["wrapped"]

    def test_genuinely_infinite_payload_still_rejected(self):
        with pytest.raises(ValueError, match="Invalid circular reference"):
            parse('[["C",0]]', {"C": lambda v: v})


class TestOutOfRangeIndex:
    """JS reads past the end of the array and gets `undefined`.

    Python would wrap a negative index around to a real element, silently
    yielding the wrong value, or raise IndexError.
    """

    def test_negative_index_does_not_wrap_around(self):
        assert parse("[[-8],1,2,3,4,5,6,7,8]") == [Undefined]

    def test_index_past_end(self):
        assert parse("[[9],1]") == [Undefined]

    @pytest.mark.parametrize("index", ["1.5", "1.0"])
    def test_float_index_addresses_no_slot(self, index):
        # JS would resolve `1.0` (it has no int/float split) but nothing emits
        # a float index, and upstream only ever tests that floats are refused.
        assert parse(f'[[{index}],"a"]') == [Undefined]


class TestSparseArrayLimits:
    # `[[SPARSE,1.5]]` and `[[SPARSE,5,1.5,1]]` are upstream's own cases and
    # live in TestParseErrors. These are the integral-float forms, where the
    # port is deliberately stricter: `Number.isInteger(3.0)` is true so JS
    # happens to accept them, but nothing emits a float here and upstream only
    # ever asserts the rejections, so accepting it would be matching an
    # implementation accident.
    @pytest.mark.parametrize(
        "payload",
        [f'[[{SPARSE},3.0,0,1],"a"]', f'[[{SPARSE},5,3.0,1],"a"]'],
    )
    def test_integral_float_length_and_index_are_rejected(self, payload):
        with pytest.raises(ValueError, match="Invalid input"):
            parse(payload)

    def test_length_at_materialization_cap_allocates(self):
        result = parse(f"[[{SPARSE},{MAX_SPARSE_ARRAY_LENGTH},0,1],1]")
        assert len(result) == MAX_SPARSE_ARRAY_LENGTH

    def test_oversized_length_is_rejected_without_allocating(self):
        # A 15-byte payload must not be able to demand gigabytes.
        with pytest.raises(ValueError, match="exceeds the maximum"):
            parse(f"[[{SPARSE},{MAX_ARRAY_LEN}]]")


class TestReducerTruthiness:
    """A reducer claims a value by returning something JS-truthy.

    Empty containers are truthy in JS but falsy in Python, so the two
    disagree about whether the reducer handled the value.
    """

    # Expected strings are what JS devalue emits for the same reducer.
    @pytest.mark.parametrize(
        ("reduced", "expected"),
        [
            ({}, '[["Foo",1],{}]'),
            ([], '[["Foo",1],[]]'),
            (set(), '[["Foo",1],["Set"]]'),
            ("0", '[["Foo",1],"0"]'),
            (-1, '[["Foo",1],-1]'),
            (float("inf"), '[["Foo",-4]]'),
        ],
    )
    def test_js_truthy_results_claim_the_value(self, reduced, expected):
        reducers = {"Foo": lambda x: reduced if isinstance(x, Foo) else None}
        assert stringify(Foo(1), reducers) == expected

    @pytest.mark.parametrize("reduced", [None, False, 0, 0.0, -0.0, "", float("nan")])
    def test_js_falsy_results_decline(self, reduced):
        reducers = {"Foo": lambda x: reduced if isinstance(x, Foo) else None}
        with pytest.raises(DevalueError, match="Cannot stringify arbitrary non-POJOs"):
            stringify(Foo(1), reducers)


class TestIndexIdentity:
    """`indexes` is keyed by `id()`, which is only unique among live objects.

    Without a strong reference a reducer's temporary can be collected and its
    address reused, aliasing a later value onto the earlier one's index.
    """

    def test_reducer_temporaries_do_not_alias(self):
        boxes = [Foo(1), Foo(2), Foo(3)]
        reducers = {"Foo": lambda x: {"v": x.value} if isinstance(x, Foo) else None}

        serialized = stringify(boxes, reducers)
        assert serialized == '[[1,4,7],["Foo",2],{"v":3},1,["Foo",5],{"v":6},2,["Foo",8],{"v":9},3]'

        revived = parse(serialized, {"Foo": lambda x: Foo(x["v"])})
        assert [f.value for f in revived] == [1, 2, 3]


class TestLargeIntegers:
    """Python ints are unbounded; JS `number` is not.

    Anything outside the safe range is a `bigint` on the JS side, so emitting
    a bare JSON number would silently round-trip as a different value.
    """

    @pytest.mark.parametrize("value", [MAX_SAFE_INTEGER, -MAX_SAFE_INTEGER, 0, 1, -1])
    def test_safe_integers_stay_bare_numbers(self, value):
        assert stringify(value) == f"[{value}]"

    @pytest.mark.parametrize(
        "value",
        [MAX_SAFE_INTEGER + 1, -MAX_SAFE_INTEGER - 1, 2**64, 123456789012345678901234567890],
    )
    def test_unsafe_integers_use_the_bigint_tag(self, value):
        assert stringify(value) == f'[["BigInt","{value}"]]'
        assert parse(stringify(value)) == value

    def test_nested_large_integer(self):
        assert parse(stringify({"n": 2**70})) == {"n": 2**70}


# ═══════════════════════════════════════════════════════════════════════════
# lossless containers for JS-only distinctions
# ═══════════════════════════════════════════════════════════════════════════


class TestJsRegExp:
    """A JS RegExp is kept as source + flags rather than compiled.

    `re` rejects patterns JS accepts, and silently changes the meaning of
    others, so compiling would both crash on valid payloads and misrepresent
    them.
    """

    @pytest.mark.parametrize(
        "source",
        [r"(?<year>\d{4})", r"\p{Letter}", r"\cJ", "[]", r"\u{1F600}"],
    )
    def test_patterns_python_cannot_compile_still_parse(self, source):
        payload = json.dumps([["RegExp", source]])
        assert parse(payload) == JsRegExp(source, "")
        with pytest.raises(re.error):
            parse(payload).compile()

    def test_js_only_flags_survive(self):
        result = parse('[["RegExp","a+","gimsuy"]]')
        assert result.flags == "gimsuy"
        # Only i/m/s have Python counterparts; g/u/y are dropped on compile.
        assert result.compile().flags & (re.IGNORECASE | re.MULTILINE | re.DOTALL)

    def test_round_trips_verbatim(self):
        original = JsRegExp(r"(?<y>\d+)", "gu")
        assert parse(stringify(original)) == original

    def test_equality_is_by_source_and_flags(self):
        assert JsRegExp("a", "g") == JsRegExp("a", "g")
        assert JsRegExp("a", "g") != JsRegExp("a", "")
        assert JsRegExp("a", "g") != JsRegExp("b", "g")


class TestJsBigInt:
    """`bigint` is a distinct JS type, not just a large integer."""

    def test_parses_to_jsbigint(self):
        assert parse('[["BigInt","1"]]') == JsBigInt(1)
        assert isinstance(parse('[["BigInt","1"]]'), JsBigInt)

    def test_behaves_like_an_int(self):
        assert JsBigInt(6) + 1 == 7
        assert isinstance(JsBigInt(6), int)

    def test_small_bigint_keeps_its_tag(self):
        # A plain `1` would come back as a JS number, losing the distinction.
        assert stringify(JsBigInt(1)) == '[["BigInt","1"]]'
        assert stringify(1) == "[1]"

    def test_not_deduplicated_against_a_plain_int(self):
        # JS Map keys use SameValueZero, where `1n !== 1`.
        assert stringify([JsBigInt(1), 1]) == '[[1,2],["BigInt","1"],1]'


class TestHole:
    """An array hole is distinct from a slot holding `undefined`."""

    def test_hole_and_undefined_parse_differently(self):
        assert parse(f"[[{HOLE},1],1]") == [Hole, 1]
        assert parse(f"[[{UNDEFINED},1],1]") == [Undefined, 1]

    def test_hole_round_trips_as_a_hole(self):
        assert stringify([Hole, 1]) == f"[[{HOLE},1],1]"
        assert stringify([Undefined, 1]) == f"[[{UNDEFINED},1],1]"

    def test_hole_outside_an_array_is_rejected(self):
        with pytest.raises(DevalueError, match="hole outside an array"):
            stringify(Hole)
        with pytest.raises(DevalueError, match="hole outside an array"):
            stringify({"a": Hole})


class TestBoxedSentinels:
    """`["Object", <negative sentinel>]` is a valid payload.

    Regression: the Object branch did its own raw `values[...]` lookup, so a
    boxed NaN/Infinity/-0 raised IndexError instead of unboxing. Found by
    running upstream's own fixtures.
    """

    @pytest.mark.parametrize(
        ("payload", "check"),
        [
            (f'[["Object",{NAN}]]', lambda v: math.isnan(v)),
            (f'[["Object",{POSITIVE_INFINITY}]]', lambda v: v == float("inf")),
            (f'[["Object",{NEGATIVE_INFINITY}]]', lambda v: v == float("-inf")),
            (f'[["Object",{NEGATIVE_ZERO}]]', lambda v: math.copysign(1, v) < 0),
            (f'[["Object",{UNDEFINED}]]', lambda v: v is Undefined),
        ],
    )
    def test_boxed_sentinel_unboxes(self, payload, check):
        assert check(parse(payload))


class TestMapLimitations:
    """A JS ``Map`` lands as a ``dict``, which is lossier than it looks.

    Insertion order is fine — ``dict`` has preserved it since 3.7 — but key
    identity is not: ``dict`` uses ``__hash__``/``__eq__`` where JS uses
    SameValueZero, and it cannot hold an unhashable key at all. Recorded here
    because two of these lose data with no error at all.
    """

    def test_string_keys_lose_only_the_map_tag(self):
        result = parse('[["Map",1,2,3,4],"a",1,"b",2]')
        assert result == {"a": 1, "b": 2}
        # Re-encodes as a plain object, so the far side no longer sees a Map.
        assert stringify(result) == '[{"a":1,"b":2},1,2]'

    def test_insertion_order_is_preserved(self):
        assert list(parse('[["Map",1,2,3,4],"z",1,"a",2]')) == ["z", "a"]

    def test_bool_and_int_keys_collapse_silently(self):
        # `Map([[1, "one"], [true, "true"]])` has two entries in JS, but
        # `True == 1` and `hash(True) == hash(1)`, so one value is destroyed
        # without any error being raised.
        result = parse('[["Map",1,2,3,4],1,"one",true,"true"]')
        assert result == {1: "true"}
        assert "one" not in result.values()

    def test_non_string_keys_cannot_be_re_encoded(self):
        result = parse('[["Map",1,2],1,"a"]')
        assert result == {1: "a"}
        with pytest.raises(DevalueError, match="non-string keys"):
            stringify(result)

    def test_unhashable_keys_cannot_be_parsed(self):
        with pytest.raises(TypeError, match="unhashable"):
            parse('[["Map",1,3],{"id":2},1,"v"]')
