
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