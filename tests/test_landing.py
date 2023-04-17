from glue_job_libs import bookmarks
from appsflyer import sync, job_config


job_config = job_config.JobConfig()

state = bookmarks.State(
    state_file_bucket=job_config.glue_bucket,
    state_file_key=job_config.job_state_file
)

sync.run(job_config, state)




