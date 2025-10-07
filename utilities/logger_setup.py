import asyncio
import os
import json
import logging
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from functools import wraps
from contextlib import contextmanager


class ColoredConsoleFormatter(logging.Formatter):
	"""
	Formatter that adds colors to console output based on log levels.
	"""
	# ANSI color codes
	COLORS = {
		'DEBUG': '\033[36m',  # Cyan
		'INFO': '\033[32m',  # Green
		'WARNING': '\033[33m',  # Yellow
		'ERROR': '\033[31m',  # Red
		'CRITICAL': '\033[35m',  # Magenta
	}
	RESET = '\033[0m'

	def format(self, record):
		log_color = self.COLORS.get(record.levelname, '')
		record.levelname = f"{log_color}{record.levelname}{self.RESET}"
		return super().format(record)


class IndentedFormatter(logging.Formatter):
	"""
	Custom logging formatter to place the log message on a new line and indent it.
	"""

	def __init__(self, fmt=None, datefmt=None, style='%', validate=True, indent_size=25):
		super().__init__(fmt, datefmt, style, validate)
		self.indent_size = indent_size

	def format(self, record):
		# Process the log message using the base formatter
		original_message = super().format(record)

		# Add a newline and indent the log message part
		if ": " in original_message:
			# Split the log parts: 'timestamp [level]:' and 'message'
			parts = original_message.split(": ", 1)
			indent = " " * self.indent_size
			formatted_message = f"{parts[0]}:\n{indent}{parts[1]}"
		else:
			formatted_message = original_message  # Fallback if format is unexpected

		return formatted_message


class JSONFormatter(logging.Formatter):
	"""
	Formatter that outputs log records as JSON objects.
	"""

	def format(self, record):
		log_entry = {
			'timestamp': datetime.fromtimestamp(record.created).isoformat() + 'Z',
			'level': record.levelname,
			'logger': record.name,
			'message': record.getMessage(),
			'module': record.module,
			'function': record.funcName,
			'line': record.lineno,
			'thread': record.thread,
			'thread_name': record.threadName,
		}

		# Add exception info if present
		if record.exc_info:
			log_entry['exception'] = self.formatException(record.exc_info)

		# Add extra fields
		for key, value in record.__dict__.items():
			if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
						   'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
						   'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
						   'processName', 'process', 'message']:
				log_entry[key] = value

		return json.dumps(log_entry)


class LogFilter:
	"""
	Custom log filter to include/exclude messages based on criteria.
	"""

	def __init__(self, include_patterns: List[str] = None, exclude_patterns: List[str] = None):
		self.include_patterns = include_patterns or []
		self.exclude_patterns = exclude_patterns or []

	def filter(self, record):
		message = record.getMessage()

		# If include patterns are specified, message must match at least one
		if self.include_patterns:
			if not any(pattern in message for pattern in self.include_patterns):
				return False

		# If exclude patterns are specified, message must not match any
		if self.exclude_patterns:
			if any(pattern in message for pattern in self.exclude_patterns):
				return False

		return True


class PerformanceLogger:
	"""
	Context manager and decorator for measuring execution time.
	"""

	def __init__(self, logger: logging.Logger, operation_name: str):
		self.logger = logger
		self.operation_name = operation_name
		self.start_time = None

	def __enter__(self):
		self.start_time = datetime.now()
		self.logger.debug(f"Starting operation: {self.operation_name}")
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		end_time = datetime.now()
		duration = (end_time - self.start_time).total_seconds()
		self.logger.info(f"Operation '{self.operation_name}' completed in {duration:.4f}s")


class LoggerManager:
	"""
	Centralized logger management system.
	"""
	_instance = None
	_lock = threading.Lock()

	def __new__(cls):
		if cls._instance is None:
			with cls._lock:
				if cls._instance is None:
					cls._instance = super().__new__(cls)
		return cls._instance

	def __init__(self):
		if not hasattr(self, 'initialized'):
			self.loggers: Dict[str, logging.Logger] = {}
			self.config: Dict[str, Any] = {}
			self.log_hooks: List[Callable] = []
			self.initialized = True

	def add_hook(self, hook_func: Callable[[logging.LogRecord], None]):
		"""Add a hook function that will be called for every log record."""
		self.log_hooks.append(hook_func)

	def remove_hook(self, hook_func: Callable[[logging.LogRecord], None]):
		"""Remove a previously added hook function."""
		if hook_func in self.log_hooks:
			self.log_hooks.remove(hook_func)

	def get_all_loggers(self) -> Dict[str, logging.Logger]:
		"""Get all managed loggers."""
		return self.loggers.copy()

	def set_global_level(self, level: int):
		"""Set logging level for all managed loggers."""
		for logger in self.loggers.values():
			logger.setLevel(level)

	def cleanup_old_logs(self, log_dir: str = "logs", days_to_keep: int = 30):
		"""Remove log files older than specified days."""
		if not os.path.exists(log_dir):
			return

		cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)

		for filename in os.listdir(log_dir):
			filepath = os.path.join(log_dir, filename)
			if os.path.isfile(filepath) and filename.endswith('.log'):
				if os.path.getmtime(filepath) < cutoff_time:
					try:
						os.remove(filepath)
						print(f"Removed old log file: {filename}")
					except OSError as e:
						print(f"Failed to remove {filename}: {e}")


class HookHandler(logging.Handler):
	"""
	Custom handler that triggers registered hooks.
	"""

	def __init__(self, hooks: List[Callable]):
		super().__init__()
		self.hooks = hooks

	def emit(self, record):
		for hook in self.hooks:
			try:
				hook(record)
			except Exception:
				pass  # Don't let hook failures break logging


def get_logger(
		module_name: str,
		log_dir: str = "logs",
		level: int = logging.DEBUG,
		console_output: bool = True,
		file_output: bool = True,
		json_format: bool = False,
		colored_console: bool = True,
		max_file_size: int = 5 * 1024 * 1024,  # 5 MB
		backup_count: int = 3,
		rotation_type: str = "size",  # "size" or "time"
		time_rotation: str = "midnight",  # for time-based rotation
		custom_format: Optional[str] = None,
		filters: Optional[LogFilter] = None,
		extra_handlers: Optional[List[logging.Handler]] = None
) -> logging.Logger:
	"""
	Enhanced logger factory with multiple configuration options.

	:param module_name: The name of the module using the logger.
	:param log_dir: Directory for log files.
	:param level: Logging level.
	:param console_output: Whether to output to console.
	:param file_output: Whether to output to file.
	:param json_format: Whether to use JSON formatting for file output.
	:param colored_console: Whether to use colored console output.
	:param max_file_size: Maximum size per log file (for size-based rotation).
	:param backup_count: Number of backup files to keep.
	:param rotation_type: Type of rotation ("size" or "time").
	:param time_rotation: Time-based rotation interval.
	:param custom_format: Custom log format string.
	:param filters: Custom log filter.
	:param extra_handlers: Additional handlers to add.
	:return: Configured logger instance.
	"""
	manager = LoggerManager()

	# Return existing logger if already configured
	if module_name in manager.loggers:
		return manager.loggers[module_name]

	# Ensure the logs directory exists
	if file_output:
		os.makedirs(log_dir, exist_ok=True)

	# Create logger instance
	logger = logging.getLogger(module_name)
	logger.setLevel(level)

	# Clear existing handlers to avoid duplicates
	logger.handlers.clear()

	# Default format
	default_format = custom_format or "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

	# File handler
	if file_output:
		log_file = os.path.join(log_dir, f"{module_name.replace('.', '_')}.log")

		if rotation_type == "time":
			file_handler = TimedRotatingFileHandler(
				filename=log_file,
				when=time_rotation,
				backupCount=backup_count,
				encoding="utf-8"
			)
		else:  # size-based rotation
			file_handler = RotatingFileHandler(
				filename=log_file,
				maxBytes=max_file_size,
				backupCount=backup_count,
				encoding="utf-8"
			)

		# Choose formatter
		if json_format:
			file_formatter = JSONFormatter()
		else:
			file_formatter = IndentedFormatter(default_format)

		file_handler.setFormatter(file_formatter)

		# Add filter if provided
		if filters:
			file_handler.addFilter(filters.filter)

		logger.addHandler(file_handler)

	# Console handler
	if console_output:
		console_handler = logging.StreamHandler()

		if colored_console:
			console_formatter = ColoredConsoleFormatter(default_format)
		else:
			console_formatter = IndentedFormatter(default_format)

		console_handler.setFormatter(console_formatter)

		# Add filter if provided
		if filters:
			console_handler.addFilter(filters.filter)

		logger.addHandler(console_handler)

	# Add hook handler for registered hooks
	if manager.log_hooks:
		hook_handler = HookHandler(manager.log_hooks)
		logger.addHandler(hook_handler)

	# Add extra handlers
	if extra_handlers:
		for handler in extra_handlers:
			logger.addHandler(handler)

	# Store in manager
	manager.loggers[module_name] = logger

	return logger


def log_performance(operation_name: str = None):
	"""
	Decorator to log function execution time for both sync and async functions.
	"""

	def decorator(func):
		if asyncio.iscoroutinefunction(func):
			# Handle async functions
			@wraps(func)
			async def async_wrapper(*args, **kwargs):
				logger = logging.getLogger(func.__module__)
				op_name = operation_name or f"{func.__name__}"

				with PerformanceLogger(logger, op_name):
					return await func(*args, **kwargs)

			return async_wrapper
		else:
			# Handle sync functions (original code)
			@wraps(func)
			def wrapper(*args, **kwargs):
				logger = logging.getLogger(func.__module__)
				op_name = operation_name or f"{func.__name__}"

				with PerformanceLogger(logger, op_name):
					return func(*args, **kwargs)

			return wrapper

	return decorator


@contextmanager
def log_context(logger: logging.Logger, operation_name: str, level: int = logging.INFO):
	"""
	Context manager for logging operation start and end.
	"""
	logger.log(level, f"Starting: {operation_name}")
	start_time = datetime.now()

	try:
		yield
		duration = (datetime.now() - start_time).total_seconds()
		logger.log(level, f"Completed: {operation_name} (took {duration:.4f}s)")
	except Exception as e:
		duration = (datetime.now() - start_time).total_seconds()
		logger.error(f"Failed: {operation_name} after {duration:.4f}s - {str(e)}")
		raise


def setup_application_logging(
		app_name: str,
		log_level: int = logging.INFO,
		log_dir: str = "logs",
		enable_performance_logging: bool = True,
		max_file_size: int = 10 * 1024 * 1024,  # 10 MB
		backup_count: int = 5
) -> logging.Logger:
	"""
	Set up application-wide logging configuration.

	:param app_name: Name of the application.
	:param log_level: Global logging level.
	:param log_dir: Directory for log files.
	:param enable_performance_logging: Whether to enable performance logging.
	:param max_file_size: Maximum size per log file.
	:param backup_count: Number of backup files to keep.
	:return: Main application logger.
	"""
	# Create main application logger
	main_logger = get_logger(
		module_name=app_name,
		level=log_level,
		log_dir=log_dir,
		max_file_size=max_file_size,
		backup_count=backup_count,
		colored_console=True,
		json_format=False
	)

	# Add performance logging if enabled
	if enable_performance_logging:
		perf_logger = get_logger(
			module_name=f"{app_name}.performance",
			level=logging.DEBUG,
			log_dir=log_dir,
			console_output=False,  # Performance logs go to file only
			json_format=True  # JSON format for easier parsing
		)

	main_logger.info(f"Application logging initialized for: {app_name}")

	return main_logger


# Convenience functions for quick logging setup
def get_simple_logger(name: str) -> logging.Logger:
	"""Get a simple logger with basic configuration."""
	return get_logger(name, level=logging.INFO)


def get_debug_logger(name: str) -> logging.Logger:
	"""Get a debug logger with verbose output."""
	return get_logger(name, level=logging.DEBUG, colored_console=True)


def get_production_logger(name: str) -> logging.Logger:
	"""Get a production logger with JSON format and no console output."""
	return get_logger(
		name,
		level=logging.WARNING,
		console_output=False,
		json_format=True,
		max_file_size=50 * 1024 * 1024,  # 50 MB
		backup_count=10
	)


# Example usage and testing
if __name__ == "__main__":
	# Example of using the enhanced logger
	logger = get_logger(__name__, colored_console=True)

	logger.debug("This is a debug message")
	logger.info("This is an info message")
	logger.warning("This is a warning message")
	logger.error("This is an error message")
	logger.critical("This is a critical message")


	# Example of performance logging
	@log_performance("test_operation")
	def test_function():
		import time
		time.sleep(1)
		return "completed"


	result = test_function()

	# Example of context logging
	with log_context(logger, "database_connection"):
		import time

		time.sleep(0.5)
		logger.info("Connected to database")