
import htcondor
import time


class Ad:
    """Condor classad fields specifically considered"""

    # global job identifier (unique between all jobs)
    id = "GlobalJobId"

    # username of submitter of the job
    owner = "Owner"

    # username and their domain (Owner@domain) of the job submitter
    user = "User"

    # total seconds of CPU use of a job
    cpu_time = "RemoteUserCpu"

    # site at which the job was submitted
    submit_site = "SUBMIT_SITE"

    # site at which the job runs
    job_site = "MATCH_EXP_JOB_SITE",

    # status of the job (numerical 1-6)
    status = "JobStatus"

    # previous status of the job (numerical 1-6)
    prev_status = "LastJobStatus"

    # total seconds of wall time (e.g. IO time) of the job
    wall_time = "RemoteWallClockTime"

    # time (seconds since epoch) of job entering queue
    queue_time = "QDate"

    # start time (seconds since epoch) of the job's FIRST run
    first_run_start_time = "JobStartDate"

    # start time (seconds since epoch) of the job's PREVIOUS (not current) run
    prev_run_start_time = "JobLastStartDate"

    # number of times the job has been started
    num_run_starts = "NumJobStarts"

    # last time (seconds since epoch) the job was evicted from running
    last_evict_time = "LastVacateTime"

    # start time (seconds since epoch) of the job's MOST RECENT (may be current) run
    last_run_start_time = "JobCurrentStartExecutingDate"    # also JobCurrentStartDate (1s dif)

    # time (seconds since epoch) of the job entering its current status
    entered_status_time = "EnteredCurrentStatus"

    # Example of a job

    # JobStatus                    (2)
    # LastJobStatus                (1)
    # NumJobStarts                 (2)

    # QDate                        (1454718023)

    # JobStartDate                 (1454718084)
    # JobLastStartDate             (1454718084)

    # LastVacateTime               (1454787327)

    # JobCurrentStartDate          (1454787679)
    # JobCurrentStartExecutingDate (1454787680)

    # EnteredCurrentStatus         (1454787679)


class Job(object):
    """A single Condor job container"""

    # required condor classad fields for a job
    fields = [
        Ad.id, Ad.submit_site, Ad.job_site, Ad.status, Ad.prev_status, Ad.num_run_starts, Ad.first_run_start_time,
        Ad.prev_run_start_time, Ad.last_evict_time, Ad.last_evict_time, Ad.entered_status_time
    ]

    class Status:
        IDLE = 1
        RUNNING = 2
        REMOVED = 3
        COMPLETED = 4
        HELD = 5
        TRANSFERRING_OUTPUT = 6

    # optimises space use of many Job instances
    # __slots__ = ('id', 'owner', 'cpu', 'submit_site', 'job_site')

    def __init__(self, ad):

        self.id = ad[Ad.id]
        self.submit_site = ad[Ad.submit_site]

        # idle jobs having never run don't have job sites
        self.job_site = ad[Ad.job_site] if (Ad.job_site in ad) else None

        # running on brick gives dud job site
        if self.job_site == "Unknown":
            self.job_site = self.submit_site
        self.status = ad[Ad.status]

        self.queue_time = ad[Ad.queue_time]

        # idle jobs having never run don't have a previous status
        self.prev_status = ad[Ad.prev_status] if (Ad.prev_status in ad) else None
        self.num_run_starts = ad[Ad.num_run_starts]

        # idle jobs having never run don't have a first run start time, or a previous
        self.first_run_start_time = ad[Ad.first_run_start_time] if (Ad.first_run_start_time in ad) else None

        # if the job has only started running once or twice (not completed twice), this will equal first
        self.prev_run_start_time = ad[Ad.prev_run_start_time] if (Ad.prev_run_start_time in ad) else None

        self.last_run_start_time = ad[Ad.last_run_start_time] if (Ad.last_run_start_time in ad) else None
        self.last_evict_time = ad[Ad.last_evict_time] if (Ad.last_evict_time in ad) else None
        self.entered_status_time = ad[Ad.entered_status_time] if (Ad.entered_status_time in ad) else None

    def is_idle(self):
        """returns whether the job is currently in the idle state"""
        return self.status == Job.Status.IDLE

    def is_running(self):
        """returns whether the job is currently in the running state"""
        return self.status == Job.Status.RUNNING

    def is_removed(self):
        """returns whether the job is currently in the removed state"""
        return self.status == Job.Status.REMOVED

    def is_completed(self):
        """returns whether the job is currently in the completed state"""
        return self.status == Job.Status.COMPLETED

    def is_held(self):
        """returns whether the job is currently in the held state"""
        return self.status == Job.Status.HELD

    def is_transferring_output(self):
        """returns whether the job is currently in the transferring output state"""
        return self.status == Job.Status.TRANSFERRING_OUTPUT

    def is_active(self):
        """returns whether the job is in an active state (state may change in future)"""
        return self.is_idle() or self.is_running() or self.is_held() or self.is_transferring_output()

    def was_idle(self):
        """returns whether the job was previously (the very previous) in the idle state"""
        return self.prev_status == Job.Status.IDLE

    def was_running(self):
        """returns whether the job was previously (the very previous) in the running state"""
        return self.prev_status == Job.Status.RUNNING

    def was_held(self):
        """returns whether the job was previously (the very previous) in the held state"""
        return self.prev_status == Job.Status.HELD

    def was_transferring_output(self):
        """returns whether the job was previously (the very previous) in the transferring output state"""
        return self.prev_status == Job.Status.TRANSFERRING_OUTPUT

    def get_time_span_idle(self):
        """
        returns the time span  (start, end) of job's most recent idle state
        (if still idle, span end is False)
        """
        if self.is_idle():

            # if the job has run previously (i.e. it has been evicted after running)
            if self.was_running():
                return self.last_evict_time, False

            # otherwise the job has been idle since queued
            return self.queue_time, False

        # if the job has only started running once, idle has been from queue to first run
        if (self.is_running() or self.was_running()) and (self.num_run_starts == 1):
            return self.queue_time, self.first_run_start_time

        # if the job has run more than once, it was idle from its last eviction to its last run
        if (self.is_running() and self.was_running()) and (self.num_run_starts > 1):
            return self.last_evict_time, self.last_run_start_time

        # otherwise, the job has been removed while idle
        if self.is_removed():
            return self.queue_time, self.entered_status_time

        raise RuntimeError(
                "get_time_span_idle didn't return anything (unexpected job statuses)\n" +
                "JobStatus: %s, LastJobStatus: %s, NumJobStarts: %s" % (self.status,
                                                                        self.prev_status,
                                                                        self.num_run_starts)
        )

    def get_time_span_running(self):
        """
        returns the time span of the job's most recent running state in format (start, end).
        start will be False if never run. end will be False if still running (and start not False)
        """
        if self.is_idle():

            # if currently idle but ran, the job was evicted
            if self.was_running():
                return self.last_run_start_time, self.last_evict_time

            # otherwise the job has been idle sinced queued and never run
            return False, False

        # if the job is running (no matter how many times it has previously run), return last start
        if self.is_running():
            return self.last_run_start_time, False

        # if the job was running (it's now removed, completed, held, transferring or re-idled):
        if self.was_running():
            return self.last_run_start_time, self.entered_status_time

        raise RuntimeError(
                "get_time_span_running didn't return anything (unexpected job statuses)\n" +
                "JobStatus: %s, LastJobStatus: %s, NumJobStarts: %s" % (self.status,
                                                                        self.prev_status,
                                                                        self.num_run_starts)
        )

    def is_idle_during(self, t0, t1):
        """
        returns whether the job was idle for any time between times t0 and t1, though only considers the
        job's most recent period of idleness (so may be technically incorrect if idle multiple times)
        """
        # i1 is False if the job is still idle
        i0, i1 = self.get_time_span_idle()
        i1 = i1 if i1 else t1

        # idle during [t0, t1] if it doesn't end before or start after
        return not (i0 > t1 or i1 < t0)

    def is_running_during(self, t0, t1):
        """
        returns whether the job was running for any time between times t0 and t1, though only considers
        the job's most recent period of running (so may be technically incorrect if has run multiple times)
        """
        # r1 is False if the job is still running
        r0, r1 = self.get_time_span_running()
        r1 = r1 if r1 else t1

        # ifle during [t0, t1] if it ever ran (r0 not False) and doesn't end before or start after
        return r0 and not (r0 > t1 or r1 < t0)

    def get_time_idle_in(self, t0, t1):
        """returns the duration (seconds) for which the job is idle within times t0 and t1"""
        # i1 is False if the job is still ide
        i0, i1 = self.get_time_span_idle()
        i1 = i1 if i1 else t1

        # time within bin appears negative if [t0, t1] & [i0, i1] are disjoint (but should be 0)
        dt = min(t1, i1) - max(t0, i0)
        return dt if dt >= 0 else 0

    def get_time_running_in(self, t0, t1):
        """returns the duration (seconds) for which the job is running within t0 to t1"""
        # r1 is False if the job is still running
        r0, r1 = self.get_time_span_running()
        r1 = r1 if r1 else t1

        # r0 is False if the job has never run
        if not r0:
            return 0

        # time within bin appears negative if [t0, t1] & [r0, r1] are disjoint (but should be 0)
        dt = min(t1, r1) - max(t0, r0)
        return dt if dt >= 0 else 0


class JobCache(object):
    """Remembers old job values"""

    def __init__(self):
        self.cache = {}

    def add(self, job, field, time):
        """adds the value of job's field to the cache under time"""
        if job.id not in self.cache:
            self.cache[job.id] = [time, {}]
        self.cache[job.id][0] = time
        self.cache[job.id][1][field] = job[field]

    def contains(self, job, field):
        """returns True if job's field is cached, otherwise False"""
        return (job.id in self.cache) and (field in self.cache[job.id][1])

    def get(self, job, field):
        """get a job field's previous value and cache time: [val, time]"""
        jobc = self.cache[job.id]
        return jobc[1][field], jobc[0]


schedd = htcondor.Schedd()
# cache = JobCache()

jobs = schedd.query("Owner=?=tysonjones", Job.fields)

'''
while True:

    jobs = map(Job, schedd.query("Owner=?=tysonjones", Job.fields))

    for job in jobs:

        print "Job (%s) for %s at %s from %s:" % (job.id, job.owner, job.job_site, job.submit_site)

        cur_val, cur_time = job.cpu_time, int(time.time())
        print "cpu is %d at %d" % (cur_val, cur_time)

        if cache.contains(job, Ad.cpu):
            prev_val, prev_time = cache.get(job, Ad.cpu)
            print "cpu was %d at %d" % (prev_val, prev_time)

            dv = cur_val - prev_val
            dt = cur_time - prev_time
            dd = dv/float(dt)
            print "change is %d in %d seconds, or %d per second" % (dv, dt, dd)

        cache.add(job, Ad.cpu, cur_time)
        print ""
'''