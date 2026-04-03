import dagster as dg


class ScheduledJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Schedules an asset job using flexible asset selection syntax.

    Accepts a cron schedule and any Dagster asset selection string
    (group, tag, kind, owner, key pattern, or combinations).
    """

    job_name: str
    cron_schedule: str
    asset_selection: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        job = dg.define_asset_job(
            name=self.job_name,
            selection=self.asset_selection,
        )
        schedule = dg.ScheduleDefinition(
            job=job,
            cron_schedule=self.cron_schedule,
        )
        return dg.Definitions(jobs=[job], schedules=[schedule])
