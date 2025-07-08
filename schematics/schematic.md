graph TD
A[Report Discovery] -->|Updates| B[report_cron_schedules Asset]
B -->|Read by| C{Scheduling Approach}
C -->|SENSOR| D[Report Cron Sensor]
C -->|SCHEDULE| E[Unified Report Schedule]
D -->|Triggers| F[Process Report Job]
E -->|Triggers| F
F -->|1| G[Report Data]
G -->|2| H[External Job Submission]
H -->|3| I[Local Job Record]
I -->|4| J[Job Completion Status]
J -->|5| K[Results Storage]
