"""Tests for vercel.cron."""

import pytest

from vercel.cron import CronSchedule, CronTab, CronTabError, cron

# --- Module-level functions for registration tests ---

_ct_string = CronTab()
_ct_string2 = CronTab()
_ct_sched = CronTab()
_ct_sched_default = CronTab()
_ct_kwarg1 = CronTab()
_ct_kwarg2 = CronTab()
_ct_kwarg_defaults = CronTab()
_ct_preserves = CronTab()
_ct_preserves_name = CronTab()
_ct_call = CronTab()


@_ct_string.register("* * * * 5")
def _job_string():
    pass


@_ct_string2.register("*/15 0 1,15 * 1-5")
def _job_string2():
    pass


_sched_obj = CronSchedule(minute="0", hour="6")


@_ct_sched.register(_sched_obj)
def _job_sched():
    pass


@_ct_sched_default.register(CronSchedule())
def _job_sched_default():
    pass


@_ct_kwarg1.register(hour=0)
def _job_kwarg1():
    pass


@_ct_kwarg2.register(minute="*/15", day_of_week=5)
def _job_kwarg2():
    pass


@_ct_kwarg_defaults.register()
def _job_kwarg_defaults():
    pass


@_ct_preserves.register("0 0 * * *")
def _job_preserves():
    return 42


@_ct_preserves_name.register(hour=0)
def _job_preserves_name():
    pass


@_ct_call.register("0 0 * * *")
def _job_daily():
    pass


@_ct_call.register(minute="*/15")
def _job_frequent():
    pass


@_ct_call.register(CronSchedule(hour=6, day_of_week=1))
def _job_weekly():
    pass


@cron("0 0 * * *")
def _cron_string():
    return "hello"


@cron(hour=6)
def _cron_kwargs():
    pass


@cron(CronSchedule(minute="*/5"))
def _cron_schedule():
    pass


# --- Tests ---


class TestCronSchedule:
    def test_defaults(self):
        s = CronSchedule()
        assert str(s) == "* * * * *"

    def test_all_fields(self):
        s = CronSchedule(minute="0", hour="6", day="1", month="1", day_of_week="0")
        assert str(s) == "0 6 1 1 0"

    def test_int_fields(self):
        s = CronSchedule(minute=30, hour=6)
        assert str(s) == "30 6 * * *"

    def test_mixed_fields(self):
        s = CronSchedule(minute="*/15", hour=0)
        assert str(s) == "*/15 0 * * *"

    def test_frozen(self):
        s = CronSchedule()
        with pytest.raises(AttributeError):
            s.minute = "0"  # type: ignore[misc]

    def test_equality(self):
        a = CronSchedule(hour="6")
        b = CronSchedule(hour="6")
        assert a == b

    def test_inequality(self):
        a = CronSchedule(hour="6")
        b = CronSchedule(hour="7")
        assert a != b


class TestCronTabRegisterString:
    def test_basic(self):
        assert len(_ct_string._jobs) == 1
        assert str(_ct_string._jobs[0][1]) == "* * * * 5"

    def test_complex_expression(self):
        assert str(_ct_string2._jobs[0][1]) == "*/15 0 1,15 * 1-5"

    def test_invalid_field_count(self):
        ct = CronTab()
        with pytest.raises(CronTabError, match="Expected 5 cron fields, got 3"):
            ct.register("* * *")


class TestCronTabRegisterSchedule:
    def test_basic(self):
        assert _ct_sched._jobs[0][1] is _sched_obj

    def test_default_schedule(self):
        assert str(_ct_sched_default._jobs[0][1]) == "* * * * *"


class TestCronTabRegisterKwargs:
    def test_single_kwarg(self):
        assert str(_ct_kwarg1._jobs[0][1]) == "* 0 * * *"

    def test_multiple_kwargs(self):
        assert str(_ct_kwarg2._jobs[0][1]) == "*/15 * * * 5"

    def test_no_args_all_defaults(self):
        assert str(_ct_kwarg_defaults._jobs[0][1]) == "* * * * *"


class TestCronTabRegisterPreservesFunction:
    def test_returns_original_function(self):
        assert _job_preserves() == 42

    def test_preserves_name(self):
        assert _job_preserves_name.__name__ == "_job_preserves_name"


class TestCronTabRejectsUnresolvable:
    def test_rejects_local_function(self):
        ct = CronTab()

        @ct.register("0 0 * * *")
        def local_job():
            pass

        with pytest.raises(CronTabError, match="only module-level functions"):
            ct.get_crons()

    def test_rejects_lambda(self):
        ct = CronTab()
        ct.register("0 0 * * *")(lambda: None)
        with pytest.raises(CronTabError, match="only module-level functions"):
            ct.get_crons()

    def test_rejects_method(self):
        ct = CronTab()

        class Jobs:
            @ct.register("0 0 * * *")
            def my_job(self):
                pass

        with pytest.raises(CronTabError, match="only module-level functions"):
            ct.get_crons()


class TestCronTabCall:
    def test_empty(self):
        ct = CronTab()
        assert ct.get_crons() == []

    def test_entries(self):
        result = _ct_call.get_crons()
        module = __name__
        assert result == [
            (f"{module}:_job_daily", "0 0 * * *"),
            (f"{module}:_job_frequent", "*/15 * * * *"),
            (f"{module}:_job_weekly", "* 6 * * 1"),
        ]


class TestCronDecorator:
    def test_callable(self):
        assert _cron_string() == "hello"

    def test_preserves_name(self):
        assert _cron_string.__name__ == "_cron_string"

    def test_get_crons_string(self):
        module = __name__
        assert _cron_string.get_crons() == [(f"{module}:_cron_string", "0 0 * * *")]

    def test_get_crons_kwargs(self):
        module = __name__
        assert _cron_kwargs.get_crons() == [(f"{module}:_cron_kwargs", "* 6 * * *")]

    def test_get_crons_schedule(self):
        module = __name__
        assert _cron_schedule.get_crons() == [(f"{module}:_cron_schedule", "*/5 * * * *")]

    def test_rejects_local_function(self):
        @cron("0 0 * * *")
        def local_job():
            pass

        with pytest.raises(CronTabError, match="only module-level functions"):
            local_job.get_crons()
