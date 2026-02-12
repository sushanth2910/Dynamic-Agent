import logging
import re
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional, Tuple

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger("wren-ai-service")


class EngineConfig(BaseModel):
    provider: str = "wren_ui"
    config: dict = {}


class Engine(metaclass=ABCMeta):
    @abstractmethod
    async def execute_sql(
        self,
        sql: str,
        session: aiohttp.ClientSession,
        dry_run: bool = True,
        **kwargs,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ...


def clean_generation_result(result: str) -> str:
    def _normalize_whitespace(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip()

    return (
        _normalize_whitespace(result)
        .replace("```sql", "")
        .replace("```json", "")
        .replace('"""', "")
        .replace("'''", "")
        .replace("```", "")
        .replace(";", "")
    )


def rewrite_interval_multiplication(sql: str) -> str:
    """
    Rewrite integer * INTERVAL expressions that cause
    'Cannot coerce arithmetic expression Int64 * Interval(MonthDayNano)' errors.

    Catches patterns like:
      12 * INTERVAL '1' MONTH  →  INTERVAL '12' MONTH
      12 * INTERVAL '1 month'  →  INTERVAL '12' MONTH
      N * INTERVAL '1' DAY     →  INTERVAL 'N' DAY
    """

    def _replace_match(m: re.Match) -> str:
        multiplier = m.group(1)
        unit = m.group(2).strip().upper()
        return f"INTERVAL '{multiplier}' {unit}"

    # Pattern 1: N * INTERVAL '1' UNIT  (e.g., 12 * INTERVAL '1' MONTH)
    sql = re.sub(
        r"(\d+)\s*\*\s*INTERVAL\s*'1'\s*(\w+)",
        _replace_match,
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern 2: N * INTERVAL '1 unit'  (e.g., 12 * INTERVAL '1 month')
    sql = re.sub(
        r"(\d+)\s*\*\s*INTERVAL\s*'1\s+(\w+)'",
        _replace_match,
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern 3: INTERVAL '12 month' → INTERVAL '12' MONTH (unit inside quotes)
    def _fix_unit_inside_quotes(m: re.Match) -> str:
        number = m.group(1)
        unit = m.group(2).upper()
        return f"INTERVAL '{number}' {unit}"

    sql = re.sub(
        r"INTERVAL\s*'(\d+)\s+(\w+)'",
        _fix_unit_inside_quotes,
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def remove_limit_statement(sql: str) -> str:
    pattern = r"\s*LIMIT\s+\d+(\s*;?\s*--.*|\s*;?\s*)$"
    modified_sql = re.sub(pattern, "", sql, flags=re.IGNORECASE)

    return modified_sql

