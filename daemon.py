
import htcondor
import urllib
import urllib2
import time
import json

DEBUG_PRINT = True

# TODO: metric config parsing system (just do RAW with caching and interpolation: NO change
# TODO: HOWEVER, first you should just write code to do a RAW one and see it successful on Grafana

# TODO: refactor to specify multiple values
# and format the measurement as name (tags, tags)

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


class Ad(object):
    """Condor classad fields specifically considered"""

    # global job identifier (unique between all jobs), used for caching
    id = "GlobalJobId"

    # current and previous (if exists) status of the job (numerical 1-6)
    status = "JobStatus"
    prev_status = "LastJobStatus"

    # time the job last check pointed (had some fields upddated)
    # TODO last_update_time = "LastCkptTime"
    # total seconds of CPU use of a job (sum them), reset at eviction [updated at job checkpoint or exit]
    # TODO last_cpu_user_duration = "RemoteUserCpu"
    # TODO last_cpu_sys_duration = "RemoteSysCpu"
    # total seconds job has run or was suspended, conserved over evictions [updated at job checkpoint or exit]
    # TODO all_wall_duration = "RemoteWallClockTime"
    # total seconds job has ever spent in suspension, conserved over evictions [updated at job checkpoint or exit]
    # TODO all_suspension_duration = "CumulativeSuspensionTime"
    # total seconds job has run or was suspended, reset at eviction [updated at job checkpoint or exit]
    # TODO last_wall_duration = "CommittedTime"
    # total seconds job has been suspended, reset at eviction [updated at job checkpoint or exit]
    # TODO last_suspended_duration = "CommittedSuspensionTime"
    # number of times the job has been started (not defined for standard universe) and suspended
    # TODO num_run_starts = "NumJobStarts"
    # TODO num_suspensions = "TotalSuspensions"  # conserved over evictions
    # number of cpus given to the job
    # num_cpus = "CpusProvisioned"

    # not actually needed, but relevant
    # TODO first_run_start_time = "JobStartDate"         (very first run start time)
    # TODO prev_run_start_time = "JobLastStartDate"      (previous - not current - run start time. May equal first)

    # site at which the job was submitted and ran (last), the latter requiring correction sometimes
    submit_site = "SUBMIT_SITE"
    job_site = "MATCH_EXP_JOB_Site"

    # current time
    server_time = "ServerTime"

    # time of job first entering queue
    queue_time = "QDate"

    # start times of the job's latest run
    last_run_start_time = "JobCurrentStartDate"    # ~ JobCurrentStartExecutingDate

    # last times the job was evicted (pushed back to global queue) and suspended (re-idled at machine)
    last_evict_time = "LastVacateTime"
    last_suspend_time = "LastSuspensionTime"

    # time the job entered its current status
    entered_status_time = "EnteredCurrentStatus"

    # time the job completed
    completion_date = "CompletionDate"

    # Example of a job

    # JobStatus                    (2)
    # LastJobStatus                (1)
    # NumJobStarts                 (2)

    # QDate                        (1454718023)

    # JobStartDate                 (1454718084)
    # JobLastStartDate             (1454718084)

    # LastVacateTime               (1454787327)
    # LastSuspensionTime           [never suspended]

    # JobCurrentStartDate          (1454787679)
    # JobCurrentStartExecutingDate (1454787680)

    # EnteredCurrentStatus         (1454787679)


class FileManager(object):
    """manages the parsing and writing to of all files used by the daemon"""
    FN_CONFIG = "config.json"
    FN_CACHE = "cache.json"
    FN_OUTBOX = "outbox.json"

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
        """JSON encodes (prettily) and writes the passed object obj to file filename, overwriting contents"""
        f = open(filename, 'w')
        json.dump(obj, f, indent=4)
        f.close()


class NetworkManager(object):
    """performs all networking and abstracts all HTTP use/formatting"""

    # characters which must be escaped in a measurement name
    MES_ESCAPE_CHARS = [' ', ',']

    @staticmethod
    def http_connect(url, data = False):
        """opens url, passing data and returns response. May throw network errors"""
        if data:
            data = data.replace('\n\n', '\n')
        debug_print("attempting to open %s with data:\n%s" % (url, data))
        if data:
            req = urllib2.Request(url, data)
        else:
            req = urllib2.Request(url)
        resp = urllib2.urlopen(req).read()
        debug_print("successful! response: %s" % resp)
        return resp

    @staticmethod
    def stringify_bin_data(mes, data, t):
        """
        formats data tag separated data for the bin at time t into an influxDB HTTP body, using the
        measurement name mes. data should be in the format [(val, {tag: val,...}), ...].
        """
        # no data yields empty string
        if not data:
            return ""

        # reformat the measurement name
        mes = NetworkManager._stringify_measurement(mes, [tag for tag in data[0][1]])

        body = ""
        for datum in data:
            tags = ','.join(['%s=%s' % (tag, datum[1][tag]) for tag in datum[1]])
            body += '%s,%s value=%s %s\n' % (mes, tags, datum[0], t)

        # cut off trailing newline
        return body[:-1]

    @staticmethod
    def _stringify_measurement(mes, tags):
        """reformat a measurement name to abide by influx's requirements (escaping chars) and append tags"""
        # add suffix
        if tags:
            mes += ', tagged by ' + ', '.join(tags)

        # escape illegal chars
        for char in NetworkManager.MES_ESCAPE_CHARS:
            mes = mes.replace(char, '\\'+char)
        return mes


class Outbox(object):
    """stores growing data to be pushed to the database"""
    # maximum number of time points to send influx in a single HTTP request
    HTTP_LINES_MAX = 300

    def __init__(self, config):
        """requires handles to the config (for grabbing db url) and the cache (for existing outbox)"""

        # ensure url ends with a forward slash
        self.url = config.database_url
        if self.url[-1] != '/':
            self.url += '/'

        # load outbox from file (default to empty if can't read; doesn't delete outbox)
        try:
            self.outgoing = FileManager.load_file(FileManager.FN_OUTBOX)  # { db name: "body", ...}
        except IOError:
            self.outgoing = {}

        # push previously failed data, keep failures
        self.push_outgoing()

    def add(self, db, mes, data, t):
        """adds the bin data for time t to the outbox, to be pushed to influx under measurement mes and database db"""

        # empty data ruins our formatting
        if not data:
            return

        if db in self.outgoing:
            self.outgoing[db] += "\n" + NetworkManager.stringify_bin_data(mes, data, t)
        else:
            self.outgoing[db] = NetworkManager.stringify_bin_data(mes, data, t)

    def push_outgoing(self):
        """pushes data to the database, keeps failed pushes"""
        failed = {}
        for database in self.outgoing:

            # ensure database exists (if it fails, maybe pushes to this db won't fail?)
            try:
                query = "CREATE DATABASE IF NOT EXISTS %s" % database
                NetworkManager.http_connect(self.url + 'query?' + urllib.urlencode({'q': query}))
            except urllib2.HTTPError:
                print "Error! Attempting to create database %s if nonexistant failed! Continuing..." % database

            # database exists; fragment data and push each
            args = urllib.urlencode({'db': database, 'precision': 's'})
            lines = self.outgoing[database].split('\n')                         # TODO this is lazy inefficient fragments
            for i in range(0, len(lines), Outbox.HTTP_LINES_MAX):
                fragment = '\n'.join(lines[i: i + Outbox.HTTP_LINES_MAX])

                # try to push each fragment, saving failures
                try:
                    NetworkManager.http_connect(self.url + 'write?' + args, fragment)
                except urllib2.HTTPError as e:
                    print "Error! Pushing some data to database %s at %s failed! Continuing..." % (database, self.url)

                    if database in failed:
                        failed[database] += '\n' + fragment
                    else:
                        failed[database] = fragment

        self.outgoing = failed

    def save(self):
        """save the outbox back to file"""
        FileManager.write_to_file(self.outgoing, FileManager.FN_OUTBOX)


class Cache(object):
    """stores (or assumes) previous values of a job"""
    JSON_FIELD_BIN_TIME = "NEXT INITIAL BIN START TIME"
    JSON_FIELD_JOB_VALUES = "PREVIOUS JOB VALUES"

    def __init__(self, config):
        """requires a handle to a Config instance to access a job's initial values"""

        # { field: [val, state or state boundary, field of init time], ... }
        self.initial_values = config.initial_values

        # load cache from file, recreating if unable
        try:
            j = FileManager.load_file(FileManager.FN_CACHE)
            self.first_bin_start_time = j[Cache.JSON_FIELD_BIN_TIME]  # time (applies to job_values)
            self.job_values = j[Cache.JSON_FIELD_JOB_VALUES]          # {id: (status, {field: val, ...}), ... }

        except IOError:
            self.first_bin_start_time = int(time.time()) - 60*60*24 #2*config.bin_duration
            self.job_values = {}

    @staticmethod
    def save_running_values(t, jobs, fields):
        """
        saves the cache with fields values of active jobs among passed jobs (interpolated to t)
        from current daemon run, and writes the cache back to file. Currently only correctly handles
        fields which change over time strictly when the job is in the running state
        """
        values = {}
        for job in jobs:

            # only active jobs which have ever run are to be cached (to ever be looked at again)
            if job.is_active() and job.num_run_starts > 0:
                jobvals = {}
                for field in fields:
                    jobvals[field] = job.get_running_value_at(field, t)
                values[job.id] = (job.status, jobvals)

        obj = {
            Cache.JSON_FIELD_BIN_TIME: t,
            Cache.JSON_FIELD_JOB_VALUES: values
        }
        FileManager.write_to_file(obj, FileManager.FN_CACHE)

    def get_prev_running_value_state_and_time(self, job, field):
        """
        get the job's field's previous value, the time of that value and the job's status at it.
        returns (val, time, status)
        """
        # if in the cache, return info
        if (job.id in self.job_values) and (field in self.job_values[job.id][1]):
            return self.job_values[job.id][1][field], self.job_values[job.id][0], self.first_bin_start_time,

        # otherwise we must assume an initial value for the job
        val, status_of_change, time_field = self.initial_values[field]

        # we currently only support calculation of changes in time-changing when running fields
        if status_of_change != Job.Status.String.RUNNING:
            raise RuntimeError(
                "get_prev_value_and_time was called for a field which isn't (approx) linearly increasing when " +
                "the job is running (this is currnetly the only type of changing field supported). Please seek " +
                "fields which increase strictly during the time the job is running, such as cpu time and wall time"
            )

        # some jobs don't have all the time fields (they're too young, for example)
        if time_field in job.ad:
            return val, Job.Status.RUNNING, job.ad[time_field]

        error_message = (
            "get_prev_value_and_time called for a field which wasn't yet cached, so the initial " +
            "value was used. However, the time classad field associated with the start of this " +
            "value was not present in the job!\n" +
            "field: %s, job: %s" % (time_field, json.dumps(dict(job.ad), indent=4)))
        raise RuntimeError(error_message)


class Bin(object):
    """stores, groups and calculates a metric's values for a specific time bin"""

    def __init__(self, t0, t1):
        self.start_time = t0
        self.end_time = t1

        self.sum_vals = {}               # {tag code: [{tag field: val, ...}, val], ...}
        self.job_average_vals = {}       # {tag code: [{tag field: val, ...}, val, num jobs], ...}
        self.time_average_vals = {}      # {tag code: [{tag field: val, ...}, val, total job time], ...}
        self.division_of_sums_vals = {}  # {tag code: [{tag field: val, ...}, numerator, denominator], ...}

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
            self.time_average_vals[tag_code] = [tags, val * duration, duration]

    def add_to_division_of_sums(self, num, den, tags):

        tag_code = '|'.join([tags[key] for key in tags])
        if tag_code in self.division_of_sums_vals:
            self.division_of_sums_vals[tag_code][1] += num
            self.division_of_sums_vals[tag_code][2] += den
        else:
            self.division_of_sums_vals[tag_code] = [tags, num, den]

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

    def get_division_of_sums(self):

        divisions = []
        for tag_code in self.division_of_sums_vals:
            item = self.division_of_sums_vals[tag_code]
            divisions.append((item[1] / float(item[2]), item[0]))
        return divisions


class Job(object):
    """A single Condor job container"""

    # fields that are needed by the daemon (even if not in ad) for every job, regardless of metric specific
    # (do NOT update this if you want a metric to use an ad field not currently collected; that's a 'desired' field)
    required_fields = [
        Ad.id,
        Ad.status,
        Ad.queue_time,
        Ad.entered_status_time,

        Ad.prev_status,
        Ad.server_time,

        Ad.last_run_start_time,  # TODO: check we actually need all these
        Ad.last_suspend_time,
        Ad.last_evict_time,
        Ad.completion_date
    ]

    class Status(object):
        IDLE = 1
        RUNNING = 2
        REMOVED = 3
        COMPLETED = 4
        HELD = 5
        TRANSFERRING_OUTPUT = 6

        class String(object):
            IDLE = "IDLE"
            RUNNING = "RUNNING"
            REMOVED = "REMOVED"
            COMPLETED = "COMPLETED"
            HELD = "HELD"
            TRANSFERRING_OUTPUT = "TRANSFERRING OUTPUT"

    #TODO: check these
    # optimises space use of many Job instances
    __slots__ = ('ad', 'cache',

                 'id',
                 'status',
                 'queue_time',
                 'entered_status_time',

                 'prev_status',
                 'server_time',

                 'last_run_start_time',
                 'last_suspend_time',
                 'last_evict_time',
                 'completion_date')

    def __init__(self, ad, cache):
        """requires the job's condor classad, and a handle to the global job cache"""
        self.ad = ad
        self.cache = cache

        self.id = ad[Ad.id]
        self.status = ad[Ad.status]
        self.queue_time = ad[Ad.queue_time]
        self.entered_status_time = ad[Ad.entered_status_time]

        # fresh jobs to queue don't have prev status, and condor_history jobs lack server_time
        self.prev_status = ad[Ad.prev_status] if (Ad.prev_status in ad) else None
        self.server_time = ad[Ad.server_time] if (Ad.server_time in ad) else int(time.time())

        # not all jobs have been run, suspended, evicted or completed
        self.last_run_start_time = ad[Ad.last_run_start_time] if (Ad.last_run_start_time in ad) else None
        self.last_suspend_time = ad[Ad.last_suspend_time] if (Ad.last_suspend_time in ad) else None
        self.last_evict_time = ad[Ad.last_evict_time] if (Ad.last_evict_time in ad) else None
        self.completion_date = ad[Ad.completion_date] if (Ad.completion_date in ad) else None

        # fix the shitty bad condor fields
        self.fix_ad()

    def fix_ad(self):
        """
        tinkers with some job classad fields which condor leaves invalid or problematic
        """

        # TODO: fix checkpoint dependent fields (cpu time, wall time, num starts, etc)

        # jobs run on brick don't have site 'Unknown'
        if (Ad.job_site in self.ad) and (self.ad[Ad.job_site] == "Unknown"):

            if Ad.submit_site in self.ad:
                self.ad[Ad.job_site] = self.ad[Ad.submit_site]

            else:
                raise RuntimeError("A job had an 'Unknown' MATCH_EXP_JOB_Site, so needed defaulting to its " +
                                   "submit site. The SUBMIT_SITE field however was not present in the classad! " +
                                   "This probably means get_all_required_fields wasn't called. It is needed " +
                                   "to be called, passing the desired fields to collect from jobs, such " +
                                   "that substitute fields (like submit site) are also collected.")

    def get_values(self, fields):
        """returns a dict of field name to the job's current value for all the passed fields"""
        return dict([(field, job.ad[field]) for field in fields])

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

    def get_most_recent_time_span_idle(self):
        """
        returns the time span  (start, end) of job's most recent idle state
        (if still idle, span end is False)
        """
        # if currently idle, it's been so since status change!
        if self.is_idle():
            entered = self.entered_status_time
            exited = False

        # if it was previously idle, it entered either at queue or when evicted or suspended
        elif self.was_idle():
            entered = max(self.queue_time, self.last_evict_time, self.last_suspend_time)
            exited = self.entered_status_time

        # otherwise job has been in at least two states since idle (let's assume it was just before last 2 states)
        else:
            entered = max(self.queue_time, self.last_evict_time, self.last_suspend_time)

            # if it was just running, we know when that started
            if self.was_running():
                exited = self.last_run_start_time

            # TODO: otherwise, we have no idea!
            else:
                exited = None

        # error checking
        if None in [entered, exited]:
            raise ValueError("get_most_recent_time_span_idle returned a None " +
                             "(a required classad was missing from job, " +
                             "or we couldn't determine when the idle state ended)!\n" +
                             "status: %s, prev status: %s" % (self.status, self.prev_status))

        return entered, exited

    def get_most_recent_time_span_running(self):
        """
        returns the time span of the job's most recent running state in format (start, end).
        start will be False if never run. end will be False if still running (and start not False)
        """
        # if currently running, it's been so since status change!
        if self.is_running():
            entered = self.last_run_start_time
            exited = False

        # if running previously, it ended at the status change
        elif self.was_running():
            entered = self.last_run_start_time
            exited = self.entered_status_time

        # if the job has never run, we'll return a first False
        elif not self.last_run_start_time:
            entered = False
            exited = False

        # if the job completed (and had some transfer state after running)
        elif self.is_completed():
            entered = self.last_run_start_time
            exited = self.completion_date

        # if the job is about to complete (it was held before or something), approximate ending to now state
        elif self.is_transferring_output():
            entered = self.last_run_start_time
            exited = self.entered_status_time

        # if the job was removed (whilst transferring output or something), approximate ending to removal
        elif self.is_removed():
            entered = self.last_run_start_time
            exited = self.entered_status_time

        # otherwise the job was running and was suspended or evicted
        else:
            entered = self.last_run_start_time

            # if it's been both evicted and suspended at some point, job ended at most recent after it started
            if self.last_evict_time and self.last_suspend_time:

                # if it's been both exited and suspended since starting, it ended at the earlier
                if (self.last_evict_time > entered) and (self.last_suspend_time > entered):
                    exited = min(self.last_evict_time, self.last_suspend_time)

                # if only one of eviction or suspension was after starting, it ended then
                elif self.last_evict_time > entered:
                    exited = self.last_evict_time
                elif self.last_suspend_time > entered:
                    exited = self.last_suspend_time

                # TODO: the latest eviction and suspension being before the latest job start is impossible!
                else:
                    exited = None

            # otherwise it's been either evicted or suspended
            elif self.last_evict_time and (self.last_evict_time > entered):
                exited = self.last_evict_time
            elif self.last_suspend_time and (self.last_suspend_time > entered):
                exited = self.last_suspend_time

            # TODO: it is impossible to have no evictions/suspensions after the job started running
            else:
                exited = None

        # error checking
        if None in [entered, exited]:
            raise ValueError("get_most_recent_time_span_running returned a None " +
                             "(a required classad was missing from job or " +
                             "the combination of run times and suspend/eviction times didn't make sense)!\n" +
                             "status: %s, prev status: %s" % (self.status, self.prev_status))

        return entered, exited

    def is_idle_during(self, t0, t1):
        """
        returns whether the job was idle for any time between times t0 and t1, though only considers the
        job's most recent period of idleness (so may be technically incorrect if idle multiple times)
        """
        # i1 is False if the job is still idle
        i0, i1 = self.get_most_recent_time_span_idle()
        i1 = i1 if i1 else t1

        # idle during [t0, t1] if it doesn't end before or start after
        return not (i0 >= t1 or i1 <= t0)

    def is_running_during(self, t0, t1):
        """
        returns whether the job was running for any time between times t0 and t1, though only considers
        the job's most recent period of running (so may be technically incorrect if has run multiple times)
        """
        # r1 is False if the job is still running
        r0, r1 = self.get_most_recent_time_span_running()
        r1 = r1 if r1 else t1

        # running during [t0, t1] if it ever ran (r0 not False) and doesn't end before or start after
        return r0 and not (r0 >= t1 or r1 <= t0)

    def get_time_idle_in(self, t0, t1):
        """returns the duration (seconds) for which the job is idle within times t0 and t1"""
        # i1 is False if the job is still ide
        i0, i1 = self.get_most_recent_time_span_idle()
        i1 = i1 if i1 else t1

        # time within bin appears negative if [t0, t1] & [i0, i1] are disjoint (but should be 0)
        dt = min(t1, i1) - max(t0, i0)
        return dt if dt >= 0 else 0

    def get_time_running_in(self, t0, t1):
        """returns the duration (seconds) for which the job is running within t0 to t1"""
        # r1 is False if the job is still running
        r0, r1 = self.get_most_recent_time_span_running()
        r1 = r1 if r1 else t1

        # r0 is False if the job has never run
        if not r0:
            return 0

        # time within bin appears negative if [t0, t1] & [r0, r1] are disjoint (but should be 0)
        dt = min(t1, r1) - max(t0, r0)
        return dt if dt >= 0 else 0

    def get_running_value_change_over(self, field, t0, t1):
        """gets the change (when job's running) in a field's value over time span [t0, t1], by linear interpolation"""

        # ensure that the job actually runs in the window
        dt = self.get_time_running_in(t0, t1)
        if dt == 0:
            return 0

        # get the previous known value/time from the cache (may be more recent than job's initial value)
        prev_val, prev_state, prev_time = self.cache.get_prev_running_value_state_and_time(self, field)

        # if the job wasn't RUNNING at cache time, the prev_time should be changed to when it started running again
        if prev_state != Job.Status.RUNNING:
            if self.is_running():
                prev_time = self.last_run_start_time
            else:
                # this implies the job wasn't running at prev execution end, started running then ended before now
                prev_time = self.prev_run_start_time

        # use the currently known value and find when this value was reached (end of job's run status, or now)
        next_val = self.ad[field]
        _, next_time = self.get_most_recent_time_span_running()
        # if job is still running, the value corresponds to now (server time)
        if not next_time:
            next_time = self.server_time

        # must consider for how long the job runs in the given window
        return (next_val - prev_val)/float(next_time - prev_time) * dt

    def get_running_value_at(self, field, t):
        """
        returns a field's (strictly one that increases approx linearly only when a job is running) value at time t,
        calculated by linear interpolation from a previous known value (or one assumed)
        """
        # get the previous known value/state/time from the cache (may be more recent than job's initial value)
        prev_val, prev_state, prev_time = self.cache.get_prev_running_value_state_and_time(self, field)

        # if the job wasn't running at its cache time, propogate to when it started
        if prev_state != Job.Status.RUNNING:
            if self.is_running():
                prev_time = self.last_run_start_time
            else:
                # this implies the job wasn't running at prev execution end, started running then ended before now
                prev_time = self.prev_run_start_time

        # get the currently known value and find when this value was reached (end of job's run status, or now)
        next_val = self.ad[field]
        _, next_time = self.get_most_recent_time_span_running()
        # if job is still running, the value corresponds to now (server time)
        if not next_time:
            next_time = self.server_time

        # if t is more recent than when this field stopped updating, return its final result
        if t >= next_time:
            return next_val

        # otherwise if t occurs before the value started updating again from prev, return the ol dval
        if t <= prev_time:
            return prev_val

        # otherwise linear interpolate its value at t
        return prev_val + (next_val - prev_val)/float(next_time - prev_time) * (t - prev_time)


class Config(object):
    """loads and provides access to configurable daemon settings"""

    JSON_FIELD_BIN_DURATION = "BIN DURATION"
    JSON_VALUE_BIN_DURATION_DEFAULT = 5*60

    JSON_FIELD_DATABASE_URL = "DATABASE URL"
    JSON_VALUE_DATABASE_URL_EMPTY = "enter the database's domain here"

    JSON_FIELD_COLLECTOR_ADDRESS = "COLLECTOR ADDRESS"
    JSON_VALUE_COLLECTOR_ADDRESS_LOCAL = "LOCAL"

    JSON_FIELD_JOB_CONSTRAINT = "JOB CONSTRAINT"
    JSON_VALUE_JOB_CONSTRAINT_DEFAULT = "true"

    JSON_FIELD_INIT_VALUES = "INITIAL JOB VALUES"
    JSON_VALUE_INIT_VALUES_DEFAULT = {}

    # TODO
    '''
    JSON_VALUE_INIT_VALUES_DEFAULT = {
                Ad.cpu_time: [0, Job.Status.String.RUNNING, Ad.first_run_start_time],
                Ad.wall_time: [0, Job.Status.String.RUNNING, Ad.first_run_start_time]
    }
    '''

    class DaemonMode(object):
        GRID = "GRID"
        BATCH = "BATCH"

    def __init__(self):

        # initial_values give a field's initial value in a job,
        # when that value changes and from when it is (re)initialised
        # {field: [init val, status or boundary of value change, time of initialisation]

        # try to load the config, creating with defaults otherwise
        try:
            j = FileManager.load_file(FileManager.FN_CONFIG)
            self.bin_duration = j[Config.JSON_FIELD_BIN_DURATION]
            self.database_url = j[Config.JSON_FIELD_DATABASE_URL]
            self.initial_values = j[Config.JSON_FIELD_INIT_VALUES]
            self.collector_address = j[Config.JSON_FIELD_COLLECTOR_ADDRESS]
            self.constraint = j[Config.JSON_FIELD_JOB_CONSTRAINT]

        except IOError:
            self.bin_duration = Config.JSON_VALUE_BIN_DURATION_DEFAULT
            self.database_url = Config.JSON_VALUE_DATABASE_URL_EMPTY
            self.initial_values = Config.JSON_VALUE_INIT_VALUES_DEFAULT
            self.collector_address = Config.JSON_VALUE_COLLECTOR_ADDRESS_LOCAL
            self.constraint = Config.JSON_VALUE_JOB_CONSTRAINT_DEFAULT
            obj = {
                Config.JSON_FIELD_BIN_DURATION: self.bin_duration,
                Config.JSON_FIELD_DATABASE_URL: self.database_url,
                Config.JSON_FIELD_INIT_VALUES: self.initial_values,
                Config.JSON_FIELD_COLLECTOR_ADDRESS: self.collector_address,
                Config.JSON_FIELD_JOB_CONSTRAINT: self.constraint
            }
            FileManager.write_to_file(obj, FileManager.FN_CONFIG)

        # notify and exit if daemon needs configuration
        if self.database_url == Config.JSON_VALUE_DATABASE_URL_EMPTY:
            print ("Please configure the daemon (edit %s) " % FileManager.FN_CACHE +
                   "and specify the URL at which the influx databases reside (in %s)." % Config.JSON_FIELD_DATABASE_URL +
                    "\nExiting..." )
            exit()


class Condor(object):

    def __init__(self, config):

        addr = config.collector_address
        if (addr == Config.JSON_VALUE_COLLECTOR_ADDRESS_LOCAL) or (addr.strip() == ""):
            debug_print("Contacting the local collector")
            collector = htcondor.Collector()

        else:
            debug_print("Contacting a non-local collector (%s)" % config.collector_address)
            collector = htcondor.Collector(config.collector_address)
        debug_print("Fetching schedds from collector")

        self.schedds = map(htcondor.Schedd, collector.locateAll(htcondor.DaemonTypes.Schedd))
        self.constraint = config.constraint

        # will be updated once jobs are requested (may use server_time from condor_q)
        self.current_time = int(time.time())

    @staticmethod
    def _get_all_required_fields(desired_fields):
        """
        wanting some fields may require others (e.g. for classad tinkering). This method returns a list of
        all required fields for a job, given a list of those desired by metric specifiers
        """

        # TODO: actually make the rest of the code use this

        required = list(Job.required_fields) + list(desired_fields)

        # job site is 'Unknown' when it runs on the brick, requiring default to submit site
        if (Ad.job_site in desired_fields) and (Ad.submit_site not in desired_fields):
            required.append(Ad.submit_site)

        # TODO: get fields required for fixing checkpoint dependent fields (e.g. ckpt time)

        return required

    def get_jobs(self, cache, desired_fields):

        required_fields = Condor._get_all_required_fields(desired_fields)

        history_constraint = "((%s) && (EnteredCurrentStatus > %s))" % (self.constraint, cache.first_bin_start_time)

        # we want unique jobs (no double counting)
        jobs = {}
        for schedd in self.schedds:
            for ad in schedd.query(self.constraint, required_fields):
                job = Job(ad, cache)
                jobs[job.id] = job
                self.current_time = job.server_time
            for ad in schedd.history(history_constraint, required_fields, 10000):
                job = Job(ad, cache)
                jobs[job.id] = job

        return [jobs[id] for id in jobs]


def debug_print(msg):
    if DEBUG_PRINT:
        print msg


def dummy_job_test():
    dummy_job_dict = {
        Ad.id: "job#123",
        Ad.submit_site: "UCSD",
        Ad.job_site: "UCR",
        Ad.status: 2,
        Ad.prev_status: 1,
        Ad.first_run_start_time: 10,
        Ad.prev_run_start_time: None,
        Ad.last_evict_time: None,
        Ad.last_run_start_time: 10,
        Ad.entered_status_time: 10,
        Ad.queue_time: 5,
        Ad.owner: "tysonjones",
        Ad.cpu_time: 4,
        Ad.wall_time: 0,    # mimic condor bug
        Ad.server_time: 15
    }
    another_dummy_job_dict = {
        Ad.id: "job#666",
        Ad.submit_site: "UCSD",
        Ad.job_site: "UCR",
        Ad.status: 1,              # it re-idled
        Ad.prev_status: 2,
        Ad.first_run_start_time: -1,
        Ad.prev_run_start_time: None,
        Ad.last_evict_time: 10,
        Ad.last_run_start_time: -1,
        Ad.entered_status_time: 10,
        Ad.queue_time: -2,
        Ad.owner: "tysonjones",
        Ad.cpu_time: 15,
        Ad.wall_time: 11,    # left running state, so condor_q fixes wall time
        Ad.server_time: 15
    }
    global jobA, jobB
    jobA = Job(dummy_job_dict, cache)
    jobB = Job(another_dummy_job_dict, cache)


# load contextual files
config = Config()
cache = Cache(config)
condor = Condor(config)
outbox = Outbox(config)

# TODO: you need to know the classad fields desired by metrics before you collect the jobs
desired_fields = [Ad.submit_site, "Owner", "SUBMIT_SITE", "MATCH_EXP_JOB_Site", ]

# get jobs
jobs = condor.get_jobs(cache, desired_fields)

# allocate time since previous run into bins
bin_times = range(cache.first_bin_start_time, condor.current_time, config.bin_duration)
if len(bin_times) < 2:
    exit()
bin_start_times, final_bin_end_time = bin_times[:-1], bin_times[-1]
del bin_times

"----------------------------------------------------------------------------------------------------------------------"

#TODO: we can only get status specific info right now (no values or val changes)
#TODO: all fields (tags) required by the below metrics are specified above in desired

# database name on the front end
db = "Daemon3TestDatabase"

# get the number of running jobs anywhere, tagged by owner (and submit site)
mes = "num running at bin end"
tags = ["SUBMIT_SITE", "Owner"]
debug_print("Processing metric: %s (tagged by %s)" % (mes, ', '.join(tags)))
for bin_start_time in bin_start_times:
    time_bin = Bin(bin_start_time, bin_start_time + config.bin_duration)
    for job in jobs:
        if job.is_running_during(time_bin.end_time - 1, time_bin.end_time):
            time_bin.add_to_sum(1, job.get_values(tags))
    results = time_bin.get_sum()
    outbox.add(db, mes, results, time_bin.start_time)

# getting the number of running jobs at the end of the bin, tagged by submit site and job site
mes = "num running at bin end"
tags = ["SUBMIT_SITE", "MATCH_EXP_JOB_Site"]
debug_print("Processing metric: %s (tagged by %s)" % (mes, ', '.join(tags)))
for bin_start_time in bin_start_times:
    time_bin = Bin(bin_start_time, bin_start_time + config.bin_duration)
    for job in jobs:
        if job.is_running_during(time_bin.end_time - 1, time_bin.end_time):       # condition of inclusion
            time_bin.add_to_sum(1, job.get_values(tags))                          # method of inclusion
    results = time_bin.get_sum()                                                  # (method of inclusion)
    outbox.add(db, mes, results, time_bin.start_time)

# get the number of idle jobs total in each bin, tagged by owner, submit site and job site
mes = "num idle total in bin"
tags = ["Owner", "SUBMIT_SITE"]
debug_print("Processing metric: %s (tagged by %s)" % (mes, ', '.join(tags)))
for bin_start_time in bin_start_times:
    time_bin = Bin(bin_start_time, bin_start_time + config.bin_duration)
    for job in jobs:
        if job.is_idle_during(time_bin.start_time, time_bin.end_time):
            time_bin.add_to_sum(1, job.get_values(tags))
    outbox.add(db, mes, time_bin.get_sum(), bin_start_time)

'''
# get the total CPU
mes = "total cpu at bin start"
tags = [Ad.owner, Ad.submit_site, Ad.job_site]
debug_print("Processing metric: %s (tagged by %s)" % (mes, ', '.join(tags)))
for bin_start_time in bin_start_times:
    time_bin = Bin(bin_start_time, bin_start_time + config.bin_duration)
    for job in jobs:
        if job.num_run_starts > 0:
            time_bin.add_to_sum(job.get_running_value_at(Ad.cpu_time, time_bin.start_time), job.get_values(tags))
    outbox.add(db, mes, time_bin.get_sum(), bin_start_time)

# get the CPU efficiency of jobs tagged by owner, submit site and job site
mes = "cpu efficiency"
tags = [Ad.owner, Ad.submit_site, Ad.job_site]
debug_print("Processing metric: %s (tagged by %s)" % (mes, ', '.join(tags)))
for bin_start_time in bin_start_times:
    time_bin = Bin(bin_start_time, bin_start_time + config.bin_duration)
    for job in jobs:
        cpu = job.get_running_value_change_over(Ad.cpu_time, time_bin.start_time, time_bin.end_time)
        wall = job.get_running_value_change_over(Ad.wall_time, time_bin.start_time, time_bin.end_time)
        if wall > 0:
            time_bin.add_to_division_of_sums(100 * cpu, wall, job.get_values(tags))
    results = time_bin.get_division_of_sums()
    outbox.add(db, mes, results, bin_start_time)
'''

"----------------------------------------------------------------------------------------------------------------------"

# push to influx
outbox.push_outgoing()
outbox.save()

# cache fields
# TODO we've disabled caching for now
'''
fields_to_cache = [Ad.cpu_time, Ad.wall_time]
Cache.save_running_values(final_bin_end_time, jobs, fields_to_cache)
'''
