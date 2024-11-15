<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB CronJob Extension
The DuckDB Cron extension adds support for scheduled query execution within DuckDB. It allows you to schedule SQL queries to run periodically using standard cron expressions while your DuckDB process is active.

> Experimental: USE AT YOUR OWN RISK!


### Cron
```sql
-- Run a query every minute
SELECT cron('SELECT COUNT(*) FROM mytable', '* * * * *') AS job_id;

-- Run a query every hour at minute 0
SELECT cron('SELECT now()', '0 * * * *') AS job_id;

-- Run a query every day at 2:30 AM
SELECT cron('CALL maintenance()', '30 2 * * *') AS job_id;
```

The function returns a job ID that can be used to manage the scheduled task.

### Listing Jobs
Use the cron_jobs() table function to view all scheduled jobs and their status:
```sql
SELECT * FROM cron_jobs();

┌─────────┬──────────────────┬────────────────┬──────────────────────────┬─────────┬──────────────────────────┬─────────────┐
│ job_id  │      query       │    schedule    │         next_run         │ status  │         last_run         │ last_result │
│ varchar │     varchar      │    varchar     │         varchar          │ varchar │         varchar          │   varchar   │
├─────────┼──────────────────┼────────────────┼──────────────────────────┼─────────┼──────────────────────────┼─────────────┤
│ task_0  │ SELECT version() │ */10 * * * * * │ Fri Nov 15 20:44:20 2024 │ Active  │ Fri Nov 15 20:44:10 2024 │ Success     │
└─────────┴──────────────────┴────────────────┴──────────────────────────┴─────────┴──────────────────────────┴─────────────┘
```

Returns:

- `job_id``: Unique identifier for the job
- `query`: The SQL query to execute
- `schedule`: The cron expression
- `next_run`: Next scheduled execution time
- `status`: Current status (Active/Inactive)
- `last_run`: Last execution timestamp
-  last_result`: Result of the last execution

### Deleting Jobs
To delete a scheduled job:
```sql
SELECT cron_delete('task_0');
```
