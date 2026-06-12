# Code-check


Role: Data Eng Expert. Terse code only. Zero explanation/fluff.
Stack: Python, Scala, Java, Airflow, Spark, SQL.

Token Efficiency Rules:
- Maximize information density. Avoid code comments unless complex.
- Streamline output: Provide raw logic immediately without prefaces.
- MANDATORY: Conclude every single response with an explicit token usage count line: "Tokens Used: ~[count]".

Technical Rules:
- Python: 3.10+, strict typing, logging, no print().
- Scala: Functional paradigm, val over var, Option over null.
- Java: 17+, record types for DTOs, try-with-resources.
- SQL: ANSI, UPPERCASE keywords, explicit JOINs, CTEs.
- Spark: Vectorized functions only. No .collect() or loops. Explicit StructType schemas. Broadcast small tables.
- Airflow: TaskFlow API, no top-level heavy imports, enforce idempotency.





Stack: Python | Scala | Java | Airflow | Spark | SQL
Token Policy: Enforce maximum token compression. Eliminate chat meta-commentary. Append "Estimated Tokens: X" to the final line of all completions.

Cmds:
- Py: black . && isort . | ruff check . | pytest
- Scala: sbt scalafmt | sbt compile | sbt test
- Java: mvn spotless:apply | mvn clean compile | mvn test

Style: Code-only or ultra-dense layout. Use type-safe Dataset[T] for JVM Spark. Trigger Spark via cloud/submit operators, never local shell execution.






Format: Conventional Commits. Imperative mood. Max 50 char subject.
Prefixes: feat(data), fix(data), refactor(data), perf(data), chore(data), ci(data).
Token Rule: Do not write block descriptions unless architectural changes demand it. End with token stats if requested.



Rules: Verify schemas/types across SQL/Py/JVM before coding. Dry-run DAGs. No destructive drops/deletes without confirmation.
Token Output: Monitor prompt context size; reject or compress large schema dumps. Append tracking metrics to response footers.


from airflow.plugins_manager import AirflowPlugin
from .nth_business_day_timetable import NthBusinessDayTimetable

class BusinessDayTimetablePlugin(AirflowPlugin):
    name = "business_day_timetable_plugin"
    timetables = [NthBusinessDayTimetable]




import pendulum
from pendulum import DateTime
from airflow.timetables.base import Timetable, DagRunInfo, DataInterval, TimeRestriction

class NthBusinessDayTimetable(Timetable):
    """Schedules DAG runs on the Nth business day (Mon-Fri) of each month."""
    
    def __init__(self, n: int = 6, hour: int = 9, tz: str = "UTC"):
        self.n = n
        self.hour = hour
        self.tz = tz

    def serialize(self) -> dict:
        return {"n": self.n, "hour": self.hour, "tz": self.tz}

    @classmethod
    def deserialize(cls, data: dict) -> "NthBusinessDayTimetable":
        return cls(n=data["n"], hour=data["hour"], tz=data["tz"])

    @property
    def summary(self) -> str:
        return f"{self.n}th business day at {self.hour}:00 ({self.tz})"

    def _nth_bday(self, year: int, month: int) -> DateTime:
        """Get Nth business day of given month."""
        count, day = 0, 0
        while count < self.n:
            day += 1
            # Fixed logic: Monday(1) to Friday(5)
            if pendulum.date(year, month, day).day_of_week <= 5:
                count += 1
        return pendulum.datetime(year, month, day, self.hour, tz=self.tz)

    def _next_month(self, year: int, month: int) -> tuple[int, int]:
        return (year + 1, 1) if month == 12 else (year, month + 1)

    def _prev_month(self, year: int, month: int) -> tuple[int, int]:
        return (year - 1, 12) if month == 1 else (year, month - 1)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        end = self._nth_bday(run_after.year, run_after.month)
        if end > run_after:
            end = self._nth_bday(*self._prev_month(run_after.year, run_after.month))
        start = self._nth_bday(*self._prev_month(end.year, end.month))
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction
    ) -> DagRunInfo | None:
        if restriction.earliest is None:
            return None

        earliest = restriction.earliest

        if last_automated_data_interval is None:
            # First run: find next Nth business day >= earliest
            next_run = self._nth_bday(earliest.year, earliest.month)
            if next_run < earliest:
                next_run = self._nth_bday(*self._next_month(earliest.year, earliest.month))
            
            # For first run, use earliest as start (not previous month's bday)
            start = earliest
        else:
            # Subsequent runs
            last_end = last_automated_data_interval.end
            next_run = self._nth_bday(*self._next_month(last_end.year, last_end.month))
            start = last_end

        # Check end date constraint
        if restriction.latest and next_run > restriction.latest:
            return None

        # Handle catchup=False
        if not restriction.catchup:
            now = pendulum.now(self.tz)
            if next_run < now:
                next_run = self._nth_bday(now.year, now.month)
                if next_run < now:
                    next_run = self._nth_bday(*self._next_month(now.year, now.month))
                # Adjust start for skipped runs
                start = self._nth_bday(*self._prev_month(next_run.year, next_run.month))
                if start < earliest:
                    start = earliest

        # Fixed syntax: Pass exact DateTime arguments directly
        return DagRunInfo.interval(start, next_run)




