"""Shared service option types for Vercel SDK sessions."""

from collections.abc import Mapping, Sequence

from vercel._internal.core.errors import VercelServiceOptionsError


class ServiceOptions:
    """Base marker class for per-service session options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type["ServiceOptions"]:
        """Return the registry key for the logical service being configured."""
        return cls


ServiceOptionsMap = dict[type[ServiceOptions], ServiceOptions]


def collect_service_options(
    service_options: Sequence[ServiceOptions] | None,
) -> ServiceOptionsMap:
    """Validate a single service-options list and key it by logical service."""
    option_map: ServiceOptionsMap = {}
    if service_options is None:
        return option_map

    for option in service_options:
        if not isinstance(option, ServiceOptions):
            raise VercelServiceOptionsError(
                "service_options must contain only ServiceOptions instances"
            )

        option_key = type(option).service_options_key()
        if option_key in option_map:
            raise VercelServiceOptionsError(
                "service_options may contain at most one object per logical service"
            )
        option_map[option_key] = option

    return option_map


def merge_service_options(
    inherited: Mapping[type[ServiceOptions], ServiceOptions],
    service_options: Sequence[ServiceOptions] | None,
) -> ServiceOptionsMap:
    """Apply a scoped option list over inherited options by logical service."""
    merged = dict(inherited)
    merged.update(collect_service_options(service_options))
    return merged
