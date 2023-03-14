"""Shared exceptions for the august integration."""

class RequireValidation(Exception):
    """Error to indicate we require validation (2fa)."""


class CannotConnect(Exception):
    """Error to indicate we cannot connect."""


class InvalidAuth(Exception):
    """Error to indicate there is invalid auth."""
