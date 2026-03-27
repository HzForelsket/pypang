class BaiduPanError(Exception):
    """Base error for the project."""


class ConfigurationError(BaiduPanError):
    """Raised when local configuration is incomplete or invalid."""


class AuthenticationError(BaiduPanError):
    """Raised when access or refresh tokens are missing or invalid."""


class ApiError(BaiduPanError):
    """Raised when the Baidu Pan API returns a non-success payload."""

    def __init__(self, message: str, *, code=None, payload=None):
        super().__init__(message)
        self.code = code
        self.payload = payload or {}
