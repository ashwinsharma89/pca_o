class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded."""
    def __init__(self, limit: str, details: dict = None):
        self.limit = limit
        self.details = details
        super().__init__(f"Rate limit exceeded: {limit}")
