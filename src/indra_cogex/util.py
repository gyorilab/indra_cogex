import codecs


def unicode_escape(s: str, attempt: int = 1, max_attempts: int = 5) -> str:
    """Remove extra escapes from unicode characters in a string

    Parameters
    ----------
    s :
        A string to remove extra escapes in unicode characters from
    attempt :
        The current attempt number.
    max_attempts :
        The maximum number of attempts to remove extra escapes.

    Returns
    -------
    :
        The string with extra escapes removed.
    """
    escaped = codecs.escape_decode(s)[0].decode()
    # No more escaping needed
    if escaped.count('\\\\u') == 0:
        return bytes(escaped, "utf-8").decode("unicode_escape")
    # Too many attempts, return the input
    if attempt >= max_attempts:
        raise UnicodeEscapeError(f"Could not remove extra escapes from {s}")
    # Try again
    return unicode_escape(escaped, attempt + 1, max_attempts)


class UnicodeEscapeError(Exception):
    pass
