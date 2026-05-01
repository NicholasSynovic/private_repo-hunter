import inspect
import logging
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, TypeVar

LOGGER_NAME = "rh"
T = TypeVar("T")


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    return logging.getLogger(name)


def _timestamped_log_name() -> str:
    return f"{LOGGER_NAME}-{time.time_ns()}.log"


def _resolve_log_file(log_path: Path | None) -> tuple[Path, str | None]:
    log_name: str = _timestamped_log_name()

    if log_path is None:
        return Path.cwd() / log_name, None

    if log_path.exists() and log_path.is_dir():
        return log_path / log_name, None

    fallback: Path = Path.cwd() / log_name
    return (
        fallback,
        f"Invalid log path '{log_path}'; defaulting to '{fallback.parent}'.",
    )


def _configure_file_logger(logger: logging.Logger, log_file: Path, level: int) -> None:
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False

    handler = logging.FileHandler(filename=log_file, mode="w", encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    logger.addHandler(handler)


def configure_logging(log_path: Path | None = None, level: int = logging.INFO) -> Path:
    logger: logging.Logger = get_logger()
    log_file, error_message = _resolve_log_file(log_path)

    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        _configure_file_logger(logger=logger, log_file=log_file, level=level)
    except OSError as exc:
        fallback_log_file: Path = Path.cwd() / _timestamped_log_name()
        fallback_log_file.parent.mkdir(parents=True, exist_ok=True)
        _configure_file_logger(logger=logger, log_file=fallback_log_file, level=level)
        logger.error(
            "Failed to configure log path '%s': %s. Falling back to '%s'.",
            log_path,
            exc,
            fallback_log_file,
        )
        return fallback_log_file

    if error_message is not None:
        logger.error(error_message)

    return log_file


def _format_bound_arguments(
    func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> str:
    signature = inspect.signature(func)
    bound = signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    parts: list[str] = []
    for name, value in bound.arguments.items():
        if name in {"self", "cls"}:
            continue
        parts.append(f"{name}={value!r}")

    return ", ".join(parts)


def log_method(
    func: Callable[..., T] | staticmethod | classmethod | None = None,
    *,
    level: int = logging.INFO,
) -> Callable[..., T] | staticmethod | classmethod:
    def decorator(callable_obj: Callable[..., T]) -> Callable[..., T]:
        @wraps(callable_obj)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            logger: logging.Logger = get_logger()
            arguments: str = _format_bound_arguments(callable_obj, args, kwargs)
            logger.log(level, "Calling %s(%s)", callable_obj.__qualname__, arguments)

            try:
                result: T = callable_obj(*args, **kwargs)
            except Exception:
                logger.exception("%s failed", callable_obj.__qualname__)
                raise

            logger.log(level, "%s returned %r", callable_obj.__qualname__, result)
            return result

        return wrapper

    if func is None:
        return decorator

    if isinstance(func, staticmethod):
        return staticmethod(decorator(func.__func__))

    if isinstance(func, classmethod):
        return classmethod(decorator(func.__func__))

    return decorator(func)
