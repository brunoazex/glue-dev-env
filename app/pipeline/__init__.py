import logging

from awsglue.context import GlueContext

from .report import Report
from .steps import cleanup, load, process

logger = logging.getLogger("PIPELINE")
glue_context = None


def run(glue: GlueContext) -> Report:
    global glue_context
    glue_context = glue
    report = Report()
    try:
        report.load = load.run()
        report.cleanup = cleanup.run()
        report.process = process.run()
        report.status = "success"
    except Exception as exc:
        report.additional_info = str(exc)
        report.status = "error"
    finally:
        return report
