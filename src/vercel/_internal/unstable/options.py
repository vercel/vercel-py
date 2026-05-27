"""Shared service option types for unstable SDK sessions."""

from collections.abc import Mapping, Sequence

from vercel._internal.unstable.errors import VercelServiceOptionsError


class ServiceOptions:
    """Base marker class for per-service session options."""

    __slots__ = ()


ServiceOptionsMap = dict[type[ServiceOptions], ServiceOptions]


def collect_service_options(
    service_options: Sequence[ServiceOptions] | None,
) -> ServiceOptionsMap:
    """Validate a single service-options list and key it by concrete type."""
    option_map: ServiceOptionsMap = {}
    if service_options is None:
        return option_map

    for option in service_options:
        if not isinstance(option, ServiceOptions):
            raise VercelServiceOptionsError(
                "service_options must contain only ServiceOptions instances"
            )

        option_type = type(option)
        if option_type in option_map:
            raise VercelServiceOptionsError(
                "service_options may contain at most one object per concrete type"
            )
        option_map[option_type] = option

    return option_map


def merge_service_options(
    inherited: Mapping[type[ServiceOptions], ServiceOptions],
    service_options: Sequence[ServiceOptions] | None,
) -> ServiceOptionsMap:
    """Apply a scoped option list over inherited options by concrete type."""
    merged = dict(inherited)
    merged.update(collect_service_options(service_options))
    return merged
