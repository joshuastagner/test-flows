import asyncio
from typing import Callable, Awaitable


class PermanentError(Exception):
    """Exception to signal an unrecoverable error that should abort retries."""
    pass


async def check_with_retries(
    func: Callable[[], Awaitable[None]],
    max_attempts: int,
    delay_seconds: int,
) -> None:
    """
    Retry an async function until it succeeds or max attempts reached.

    Args:
        func: Async function to call (should raise on failure, return on success)
        max_attempts: Maximum number of attempts
        delay_seconds: Seconds to wait between attempts

    Raises:
        PermanentError: If raised by func, aborts immediately without retries
        AssertionError: If all attempts fail
    """
    last_error = None
    print('####ahoy!!')

    for attempt in range(1, max_attempts + 1):
        try:
            await func()
            return  # Success
        except PermanentError:
            raise  # Don't retry permanent errors
        except (AssertionError, Exception) as e:
            print('---->', e)
            last_error = e
            if attempt < max_attempts:
                print(f"Attempt {attempt}/{max_attempts} failed, retrying in {delay_seconds}s...")
                await asyncio.sleep(delay_seconds)
            else:
                print(f"Attempt {attempt}/{max_attempts} failed, no more retries")

    raise last_error or AssertionError("All retry attempts failed")
