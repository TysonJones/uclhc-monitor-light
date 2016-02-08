
import htcondor
import time
import json


class FileManager(object):
    """manages the parsing and writing to of all files used by the daemon"""
    FN_CONFIG = "config.json"
    FN_CACHE = "cache.json"

    @staticmethod
    def load_file(filename):
        """returns the json object (as ASCII) encoded in file with name filename"""
        f = open(filename, 'r')
        j = FileManager._to_ascii(json.load(f, object_hook=FileManager._to_ascii), True)
        f.close()
        return j

    @staticmethod
    def _to_ascii(data, ignore_dicts=False):
        """used by the manager for parsing JSON objects to ASCII"""
        if isinstance(data, unicode):
            return data.encode('utf-8')
        if isinstance(data, list):
            return [FileManager._to_ascii(item, ignore_dicts=True) for item in data]
        if isinstance(data, dict) and not ignore_dicts:
            return dict([
                (FileManager._to_ascii(key, ignore_dicts=True),
                 FileManager._to_ascii(value, ignore_dicts=True))
                for key, value in data.iteritems()])
        return data

    @staticmethod
    def write_to_file(obj, filename):
        f = open(filename, 'w')
        json.dump(obj, f, indent=4)
        f.close()


class Config(object):
    """loads and provides access to configurable daemon settings"""
    JSON_FIELD_BIN_DURATION = "BIN DURATION"
    JSON_FIELD_INIT_VALUES = "INITIAL JOB VALUES"

    def __init__(self):

        self._spoof()

        j = FileManager.load_file(FileManager.FN_CONFIG)
        self.bin_duration = j[Config.JSON_FIELD_BIN_DURATION]
        self.initial_values = j[Config.JSON_FIELD_INIT_VALUES]

    def _spoof(self):
        obj = {
            Config.JSON_FIELD_BIN_DURATION: 60,
            Config.JSON_FIELD_INIT_VALUES: {
                Ad.cpu_time: [0, Ad.first_run_start_time],
                Ad.wall_time: [0, Ad.first_run_start_time],
                Ad.num_run_starts: [0, Ad.queue_time]
            }
        }
        FileManager.write_to_file(obj, FileManager.FN_CONFIG)


class Cache(object):
    """stores (or assumes) previous values of a job"""
    JSON_FIELD_BIN_TIME = "NEXT INITIAL BIN START TIME"
    JSON_FIELD_JOB_VALUES = "PREVIOUS JOB VALUES"

    def __init__(self, config):
        """requires a handle to a Config instance to access a job's initial values"""

        self._spoof()

        j = FileManager.load_file(FileManager.FN_CACHE)
        self.first_bin_start_time = j[Cache.JSON_FIELD_BIN_TIME]  # time (applies to job_values)
        self.job_values = j[Cache.JSON_FIELD_JOB_VALUES]          # {id: {field: val, ...}, ... }
        self.initial_values = config.initial_values               # { field: [val, time field], ... }

    def _spoof(self):
        obj = {
            Cache.JSON_FIELD_BIN_TIME: 1454900000,
            Cache.JSON_FIELD_JOB_VALUES: {}
        }
        FileManager.write_to_file(obj, FileManager.FN_CACHE)

    def get_prev_value_and_time(self, job, field):
        """get the previous field value and its time of a job"""
        # if in the cache, return the cached value
        if (job.id in self.job_values) and (field in self.job_values[job.id]):
            return self.job_values[job.id][field], self.first_bin_start_time

        # otherwise we must assume an initial value for the job
        val, time_field = self.initial_values[field]
        if time_field in job:
            return val, job[time_field]

        raise RuntimeError(
                "get_prev_value_and_time called for a field which wasn't yet cached, so the initial " +
                "value was used. However, the time classad field associated with the start of this " +
                "value was not present in the job!\n" +
                "field: %s, job: %s" % (field, json.dumps(job.ad, indent=4))
        )


class Bin(object):
    """stores, groups and calculates a metric's values for a specific time bin"""

    def __init__(self, t0, t1):
        self.start_time = t0
        self.end_time = t1

        self.sum_vals = {}           # {tag code: [{tag field: val, ...}, val], ...}
        self.job_average_vals = {}   # {tag code: [{tag field: val, ...}, val, num jobs], ...}
        self.time_average_vals = {}  # {tag code: [{tag field: val, ...}, val, total job time], ...}

    def add_to_sum(self, val, tags):

        tag_code = '|'.join([tags[key] for key in tags])
        if tag_code in self.sum_vals:
            self.sum_vals[tag_code][1] += val
        else:
            self.sum_vals[tag_code] = [tags, val]

    def add_to_job_average(self, val, tags):

        tag_code = '|'.join([tags[key] for key in tags])
        if tag_code in self.job_average_vals:
            self.job_average_vals[tag_code][1] += val
            self.job_average_vals[tag_code][2] += 1
        else:
            self.job_average_vals[tag_code] = [tags, val, 1]

    def add_to_time_average(self, val, tags, duration):

        tag_code = '|'.join([tags[key] for key in tags])
        if tag_code in self.time_average_vals:
            self.time_average_vals[tag_code][1] += val * duration
            self.time_average_vals[tag_code][2] += duration
        else:
            self.job_average_vals[tag_code] = [tags, val * duration, duration]

    def get_sum(self):

        sums = []  # [(vals, {tag field: val, ...}), ... ]
        for tag_code in self.sum_vals:
            item = self.sum_vals[tag_code]
            sums.append((item[1], item[0]))
        return sums

    def get_job_average(self):

        averages = []
        for tag_code in self.job_average_vals:
            item = self.job_average_vals[tag_code]
            averages.append((item[1] / float(item[2]), item[0]))
        return averages

    def get_time_average(self):

        averages = []
        for tag_code in self.time_average_vals:
            item = self.time_average_vals[tag_code]
            averages.append((item[1] / float(item[2]), item[0]))
        return averages


class Ad(object):
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
    job_site = "MATCH_EXP_JOB_Site"

    # status of the job (numerical 1-6)
    status = "JobStatus"

    # previous status of the job (numerical 1-6)
    prev_status = "LastJobStatus"

    # current time on the Condor server
    server_time = "ServerTime"

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
    req_fields = [
        Ad.id, Ad.submit_site, Ad.job_site, Ad.status, Ad.prev_status, Ad.num_run_starts,
        Ad.first_run_start_time, Ad.prev_run_start_time, Ad.last_evict_time, Ad.last_evict_time,
        Ad.entered_status_time, Ad.queue_time, Ad.owner, Ad.cpu_time, Ad.wall_time, Ad.server_time,
        Ad.last_run_start_time
    ]

    class Status(object):
        IDLE = 1
        RUNNING = 2
        REMOVED = 3
        COMPLETED = 4
        HELD = 5
        TRANSFERRING_OUTPUT = 6

    # optimises space use of many Job instances
    # __slots__ = ('id', 'owner', 'cpu', 'submit_site', 'job_site')

    def __init__(self, ad, cache):
        """requires the job's condor classad, and a handle to the global job cache"""
        self.ad = ad
        self.cache = cache

        self.status = ad[Ad.status]
        self.owner = ad[Ad.owner]
        self.cpu_time = ad[Ad.cpu_time]   # updates every 6 minutes in Condor bindings/binaries
        self.server_time = ad[Ad.server_time] if (Ad.server_time in ad) else int(time.time())

        # wall time doesn't include current running job (add it)
        if ad[Ad.status] == Job.Status.RUNNING:
            ad[Ad.wall_time] += ad[Ad.server_time] - ad[Ad.last_run_start_time]
        self.wall_time = ad[Ad.wall_time]

        self.id = ad[Ad.id]
        self.submit_site = ad[Ad.submit_site]

        # idle jobs having never run don't have job sites
        self.job_site = ad[Ad.job_site] if (Ad.job_site in ad) else None

        # running on brick gives dud job site
        if self.job_site == "Unknown":
            self.job_site = self.submit_site

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

        # otherwise the job was removed whilst idle and never ran
        if self.is_removed():
            return False, False

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
        return not (i0 >= t1 or i1 <= t0)

    def is_running_during(self, t0, t1):
        """
        returns whether the job was running for any time between times t0 and t1, though only considers
        the job's most recent period of running (so may be technically incorrect if has run multiple times)
        """
        # r1 is False if the job is still running
        r0, r1 = self.get_time_span_running()
        r1 = r1 if r1 else t1

        # running during [t0, t1] if it ever ran (r0 not False) and doesn't end before or start after
        return r0 and not (r0 >= t1 or r1 <= t0)

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

    def get_value_change_over(self, field, t0, t1):
        """returns the change in a field's value over time span [t0, t1], by linear interpolation"""
        prev_val, prev_time = self.cache.get_prev_value_and_time(self, field)
        curr_val, curr_time = self.ad[field], self.server_time
        return (curr_val - prev_val)/float(curr_time - prev_time) * (t1 - t0)

    def get_value_at(self, field, t):
        """returns a field's value at time t, calculated by linear interpolation from a previous known value"""
        prev_val, prev_time = self.cache.get_prev_value_and_time(self, field)
        curr_val, curr_time = self.ad[field], self.server_time
        return prev_val + (curr_val - prev_val)/float(curr_time - prev_time) * (t - prev_time)


config = Config()
cache = Cache(config)


schedd = htcondor.Schedd()
const = 'Owner=?="tysonjones"'
jobs = []
now = int(time.time())
for job in schedd.query(const, Job.req_fields):
    jobs.append(Job(job, cache))
    now = job[Ad.server_time]
for job in schedd.history(const, Job.req_fields, 99999999):
    jobs.append(Job(job, cache))

bin_times = range(cache.first_bin_start_time, now, config.bin_duration)
bin_start_times, final_bin_end_time = bin_times[:-1], bin_times[-1]
del bin_times

# getting the number of running jobs at the end of the bin, tagged by owner and submit site
tags = [Ad.owner, Ad.submit_site]
for bin_start_time in bin_start_times:
    bin = Bin(bin_start_time, bin_start_time + config.bin_duration)

    for job in jobs:
        if job.is_running_during(bin.end_time - 1, bin.end_time):
            bin.add_to_sum(1, dict([(tag, job.ad[tag]) for tag in tags]))

    results = bin.get_sum()
    print "bin %s:" % bin_start_time
    for result in results:
        print "%s running jobs submitted by %s at %s" % (result[0], result[1][Ad.owner], result[1][Ad.submit_site])




'''
while True:

    f = open('log.txt', 'a')



    for job in jobs:

        f.write("Job (%s) for %s at %s from %s:" % (job.id, job.owner, job.job_site, job.submit_site))

        cur_val, cur_time = job.cpu_time, int(time.time())
        f.write("cpu is %d at %d" % (cur_val, cur_time))

        if cache.contains(job, Ad.cpu_time):
            prev_val, prev_time = cache.get(job, Ad.cpu_time)
            f.write("cpu was %d at %d" % (prev_val, prev_time))

            dv = cur_val - prev_val
            dt = cur_time - prev_time
            dd = dv/float(dt)
            f.write("change is %s in %s seconds, or %s per second" % (dv, dt, dd))

        cache.add(job, Ad.cpu_time, cur_time)
        f.write("")

    f.close()

    time.sleep(60*15)
'''