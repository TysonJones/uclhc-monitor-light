
"""
PITFALLS

- Last status checks can be before eviction times, but run information is lost from classad on eviction
(specifically, CommitedTime, RemoteWallClockTime, etc. Check the Condor manual to see
http://research.cs.wisc.edu/htcondor/manual/v8.4/condor-V8_4_4-Manual.pdf)
-> avoid correlating these

- NumJobStarts may be completely unreliable (not used in standard universe jobs)
-> avoid

- MATCH_EXP_JOB_Site will be 'Unknown' if the job runs on the brick
-> check if so and set to be SUBMIT_SITE

- RemoteUserCpu updates only when the Job check-points.

- CurrentTime is bugged
-> server time is used

- Only jobs from Condor_q have ServerTime
-> python time substituted

- RemoteWallClockTime is not updated until job stops running (suspended, terminated or completed. Dunno about evicted)
-> is updated to include current job runtime

- RemoteWallClockTime includes CumulativeSuspensionTime

- RemoteWallClockTime is not reset when the job is evicted and runs on a new machine (never reset for a job)

- CumulativeSuspensionTime does not include time the job spent idle when evicted or first idle

- NumJobStarts is not updated until running job stops (as for RemoteWallClockTime)
-> is incremented if job currently running

"""


"""
QUESTIONS

- Does suspension count as a restart (in NumJobStarts), like eviction?

"""

class Ad:
    # time the job last check pointed (had some fields upddated)
    # TODO last_update_time = "LastCkptTime"
    # total seconds job has run or was suspended, conserved over evictions [updated at job checkpoint or exit]
    # TODO all_wall_duration = "RemoteWallClockTime"
    # total seconds job has ever spent in suspension, conserved over evictions [updated at job checkpoint or exit]
    # TODO all_suspension_duration = "CumulativeSuspensionTime"
    # total seconds job has run or was suspended, reset at eviction [updated at job checkpoint or exit]
    # TODO last_wall_duration = "CommittedTime"      # ISN'T UPDATED UNTIL IN CONDOR_HISTORY (NEVER UPDATES LIVE)
    # total seconds job has been suspended, reset at eviction [updated at job checkpoint or exit]
    # TODO last_suspended_duration = "CommittedSuspensionTime"         # (NEVER UPDATES LIVE)
    # number of times the job has been started (not defined for standard universe) and suspended
    # TODO num_run_starts = "NumJobStarts"
    # TODO num_suspensions = "TotalSuspensions"  # conserved over evictions
    # number of cpus given to the job
    # num_cpus = "CpusProvisioned"
    pass