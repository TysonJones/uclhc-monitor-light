#!/usr/bin/env python

# Author:       Tyson Jones, January 2016 (MURPA student of Prof Frank Wuerthwein, UCSD).
#               Feel free to contact me at  tjon14@student.monash.edu

# Purpose:      Condorflux daemon; a condor probe for aggregating metric data into influx and grafana

import htcondor
import urllib2
import inspect
import urllib
import time
import json
import sys
import re

DEBUG_PRINT = True

class MockAd(object):
    """fields which can be specified by metrics but aren't actually Condor classads; they're manipulations thereof"""

    # the batch system (no glidein) doesn't have a pretty job site ad. We instead string manipulate the remote site
    batch_job_site = "BATCH_JOB_SITE"

    # the batch system (no glidein) doesn't have a pretty submit site ad. We instead use the schedd name
    batch_submit_site = "BATCH_SUBMIT_SITE"


class Ad(object):
    """Condor classad fields specifically considered"""

    # global job identifier (unique between all jobs), used for caching
    id = "GlobalJobId"

    # current and previous (if exists) status of the job (numerical 1-6)
    status = "JobStatus"
    prev_status = "LastJobStatus"

    # used in config's default initial values
    first_run_start_time = "JobStartDate"        # (very first run start time)
    remote_user_cpu_duration = "RemoteUserCpu"      # total seconds of CPU use of a job (sum them)
    remote_sys_cpu_duration = "RemoteSysCpu"        # these are updated at job checkpoint, reset at eviction

    # TODO prev_run_start_time = "JobLastStartDate"      (previous - not current - run start time. May equal first)

    # site at which the job was submitted and ran (last), the latter requiring correction sometimes
    submit_site = "SUBMIT_SITE"
    job_site = "MATCH_EXP_JOB_Site"

    # batch system uses this to replace the job site MockAd field
    remote_host = "RemoteHost"
    last_remote_host = "LastRemoteHost"

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
    FN_METRICS = "metrics.py"

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
    def write_json_to_file(obj, filename):
        """JSON encodes (prettily) and writes the passed object obj to file filename, overwriting contents"""
        f = open(filename, 'w')
        json.dump(obj, f, indent=4)
        f.close()

    @staticmethod
    def write_str_to_file(string, filename):
        f = open(filename, 'w')
        f.write(string)
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
        debug_print("attempting to open %s" % url)
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
            tags = ','.join(['%s=%s' % (
                NetworkManager._stringify_tag_name_or_val(tag),
                NetworkManager._stringify_tag_name_or_val(datum[1][tag]))
                             for tag in datum[1]])
            body += '%s,%s value=%s %s\n' % (mes, tags, datum[0], t)

        # cut off trailing newline
        return body[:-1]

    @staticmethod
    def _stringify_measurement(mes, tags):
        """reformat a measurement name to abide by influx's requirements (escaping chars) and append tags"""
        # add suffix
        if tags:
            mes += ' (' + ', '.join(tags) + ')'

        # escape illegal chars
        for char in NetworkManager.MES_ESCAPE_CHARS:
            mes = mes.replace(char, '\\'+char)
        return mes

    @staticmethod
    def _stringify_tag_name_or_val(string):
        # escape illegal chars
        for char in NetworkManager.MES_ESCAPE_CHARS:
            string = string.replace(char, '\\'+char)
        return string


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

        self.influx_username = config.influx_username
        self.influx_password = config.influx_password

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

        debug_print("Checking and pushing the outbox")

        failed = {}
        for database in self.outgoing:

            # ensure database exists (if it fails, maybe pushes to this db won't fail?)
            try:
                query = "CREATE DATABASE IF NOT EXISTS %s" % database
                NetworkManager.http_connect(
                        self.url + 'query?' + urllib.urlencode({'q': query,
                                                                'u': self.influx_username,
                                                                'p': self.influx_password}))
            except urllib2.HTTPError:
                print "Error! Attempting to create database %s if nonexistant failed! Continuing..." % database
            except urllib2.URLError:
                print "Error! The URL in the config (%s in %s) is bad.\nContinuing..." % (
                                Config.JSON_FIELD_DATABASE_URL,
                                FileManager.FN_CONFIG)

            # database exists; fragment data and push each
            args = urllib.urlencode(
                    {'db': database,
                     'precision': 's',
                     'u': self.influx_username,
                     'p': self.influx_password})
            lines = self.outgoing[database].split('\n')                   # TODO this is lazy inefficient fragments
            for i in range(0, len(lines), Outbox.HTTP_LINES_MAX):
                fragment = '\n'.join(lines[i: i + Outbox.HTTP_LINES_MAX])

                # try to push each fragment, saving failures
                try:
                    NetworkManager.http_connect(self.url + 'write?' + args, fragment)
                except urllib2.HTTPError as e:
                    print ("Error! Pushing some data to database %s at %s failed!\n" % (database, self.url) +
                           "(%s)\nContinuing..." % e.read())
                    if database in failed:
                        failed[database] += '\n' + fragment
                    else:
                        failed[database] = fragment
                except urllib2.URLError:
                    print ("Error! The URL in the config (%s in %s) is bad. " % (
                                Config.JSON_FIELD_DATABASE_URL,
                                FileManager.FN_CONFIG) +
                           "Continuing...")
                    if database in failed:
                        failed[database] += '\n' + fragment
                    else:
                        failed[database] = fragment

        debug_print("%s databases were attemptedly pushed to and %s failed" % (len(self.outgoing), len(failed)))
        self.outgoing = failed

    def save(self):
        """save the outbox back to file"""
        FileManager.write_json_to_file(self.outgoing, FileManager.FN_OUTBOX)


class Cache(object):
    """stores (or assumes) previous values of a job"""
    JSON_FIELD_BIN_TIME = "NEXT INITIAL BIN START TIME"
    JSON_FIELD_JOB_VALUES = "PREVIOUS JOB VALUES"

    def __init__(self, config):
        """requires a handle to a Config instance to access a job's initial values"""

        # { field: [val, state of update, field of init time], ... }
        self.initial_values = config.initial_values

        # load cache from file, recreating if unable
        try:
            j = FileManager.load_file(FileManager.FN_CACHE)
            self.first_bin_start_time = j[Cache.JSON_FIELD_BIN_TIME]  # time (applies to job_values)
            self.job_values = j[Cache.JSON_FIELD_JOB_VALUES]          # {id: (status, {field: val, ...}), ... }

        except IOError:
            self.first_bin_start_time = int(time.time()) - 60*60*1      # start looking 1h into the past
            self.job_values = {}

    @staticmethod
    def save_time_and_running_values(t, jobs, fields):
        """
        saves the cache with fields values of active jobs among passed jobs (interpolated to t)
        from current daemon run, and writes the cache back to file. Currently only correctly handles
        fields which change over time strictly when the job is in the running state (but will accept others blindly)
        """
        values = {}
        for job in jobs:

            # only active jobs which have ever run are to be cached (to ever be looked at again)
            if job.is_active() and (Ad.last_run_start_time in job.ad):
                jobvals = {}
                for field in fields:
                    jobvals[field] = job.get_value_when_running_at(field, t)
                values[job.id] = (job.status, jobvals)

        obj = {
            Cache.JSON_FIELD_BIN_TIME: t,
            Cache.JSON_FIELD_JOB_VALUES: values
        }
        FileManager.write_json_to_file(obj, FileManager.FN_CACHE)

    def get_prev_running_value_state_and_time(self, job, field):
        """
        get the job's field's previous value, the time of that value and the job's status at it.
        returns (val, status, time)
        """
        # if in the cache, return info
        if (job.id in self.job_values) and (field in self.job_values[job.id][1]):
            return self.job_values[job.id][1][field], self.job_values[job.id][0], self.first_bin_start_time,

        # otherwise we must assume an initial value for the job
        if field in self.initial_values:
            val, status_of_change, time_field = self.initial_values[field]
        else:
            raise RuntimeError(
                "Field %s hasn't had its initial value declared in the config's (%s) '%s' field! " % (
                    field,
                    FileManager.FN_CONFIG,
                    Config.JSON_FIELD_INIT_VALUES) +
                "The field was requested for a job which wasn't yet cached, so its initial value was sought but was " +
                "missing"
            )

        # TODO: we currently only support calculation of changes in time-changing when running fields
        if (status_of_change != Job.Status.String.RUNNING) and (status_of_change != Job.Status.RUNNING):
            raise RuntimeError(
                "Illegal state of field update in config's initial job values! (Only RUNNING is supported)\n" +
                "get_prev_value_and_time was called, asking for field %s " % field +
                "which wasn't cached, so the fields initial value was sought from the config. The status of change " +
                "(in what state the job must be in for the condor field to change) was declared as %s " % (
                    status_of_change) +
                "but currently only fields which update when the job is strictly running are supported (e.g. cpu time)."
            )

        # time_field mightn't yet be in the job's ad (the job hasn't yet entered the stage where the field is updated)
        if time_field in job.ad:
            return val, Job.Status.RUNNING, job.ad[time_field]
        else:
            return val, Job.Status.IDLE, False


class Bin(object):
    """stores, groups and calculates a metric's values for a specific time bin"""

    __slots__ = ('start_time',
                 'end_time',
                 'sum_vals',
                 'job_average_vals',
                 'time_average_vals',
                 'division_of_sums_vals')

    def __init__(self, t0, t1):
        self.start_time = t0
        self.end_time = t1

        self.sum_vals = {}               # {tag code: [{tag field: val, ...}, val], ...}
        self.job_average_vals = {}       # {tag code: [{tag field: val, ...}, val, num jobs], ...}
        self.time_average_vals = {}      # {tag code: [{tag field: val, ...}, val, total job time], ...}
        self.division_of_sums_vals = {}  # {tag code: [{tag field: val, ...}, numerator, denominator], ...}

    def copy(self):
        return Bin(self.start_time, self.end_time)

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

        Ad.last_run_start_time,
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

    # optimises space use of many Job instances
    __slots__ = ('ad', 'cache', 'config',

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

    def __init__(self, ad, cache, config):
        """requires the job's condor classad, and handles to the global job cache and the config"""
        self.ad = ad
        self.cache = cache
        self.config = config

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
        """
        returns a dict of field name to the job's current value for all the passed fields.
        fields can be condor classad fields (which MUST be in the job's ad) or a MockAd
        """
        values = {}
        for field in fields:

            # batch system uses last remote host
            if field == MockAd.batch_job_site:

                if Ad.last_remote_host in self.ad:
                    host = '@'.join(self.ad[Ad.last_remote_host].split('@')[1:])
                elif Ad.remote_host in self.ad:
                    host = '@'.join(self.ad[Ad.remote_host].split('@')[1:])
                else:
                    raise RuntimeError("Job.get_values contained the batch job site mock ad, but the job ad " +
                                       "didn't contain RemoteHost or LastRemoteHost!\n" +
                                       "classad:\n%s" % prettify(self.ad))

                # the host name is ugly though, so we see if it matches a rename regex
                values[field] = host
                for regexcomp in self.config.node_renames:
                    if regexcomp.match(host):
                        values[field] = self.config.node_renames[regexcomp]
                        break

            # just a regular classad field (or MockAd.batch_submit_site, which injected into the classad)
            elif field in self.ad:
                values[field] = self.ad[field]

            # this should never be called for a field not present
            else:
                raise RuntimeError("Job.get_values was called which contained a field which wasn't a MockAd " +
                                   "and wasn't in the job's classad! This probably means the user specified an " +
                                   "incorrect or mispelled condor classad field in their tags for a metric.\n" +
                                   "field: %s, classad:\n%s" % (field, prettify(self.ad)))

        return values

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

            # otherwise, we have no idea!
            else:
                exited = entered + 1

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

        # otherwise the job was running and was suspended, held or evicted
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

                # this means the job was running, then held
                else:
                    # no choice but to claim it stopped running instantly
                    exited = entered + 1

            # otherwise it's been either evicted or suspended
            elif self.last_evict_time and (self.last_evict_time > entered):
                exited = self.last_evict_time
            elif self.last_suspend_time and (self.last_suspend_time > entered):
                exited = self.last_suspend_time

            # this means the job was running, then held (no choice but to claim it stopped instantly; we lost it!)
            else:
                exited = entered + 1

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

    # TODO: this is currently ONLY for fields which update strictly during when the job is RUNNING (e.g. cpu time)
    def get_rate_of_change_of_value_when_running(self, field):
        """
        consulting the cache, gets rate of change in the value of a job's classad field.
        This is strictly for fields which update solely when the job is RUNNING
        """
        # find the previous known value (discard state)
        prev_val, _, prev_time = self.cache.get_prev_running_value_state_and_time(self, field)

        # the job might never have entered a stage where the field starts updating (so change is zero)
        if not prev_time:
            return 0

        # find the total time for which the job was running since last known value
        dt = self.get_time_running_in(prev_time, self.server_time)

        return (self.ad[field] - prev_val)/float(dt) if (dt > 0) else 0

    # TODO: this is currently ONLY for fields which update strictly during when the job is RUNNING (e.g. cpu time)
    def get_change_in_value_when_running_over(self, field, t0, t1):
        """
        consulting the cache, gets a change in the value of a job's classad field over time [t0, t1].
        This is strictly for fields which update solely when the job is RUNNING
        """
        dt = self.get_time_running_in(t0, t1)
        rate = self.get_rate_of_change_of_value_when_running(field)
        return rate * dt

    # TODO: this is currently ONLY for fields which update strictly during when the job is RUNNING (e.g. cpu time)
    def get_value_when_running_at(self, field, t):
        """
        consulting the cache, gets the value of a job's classad field at time t by linear interpolation.
        This is strictly for fields which update solely when the job is RUNNING
        """
        # find the previous known value (discard state)
        prev_val, _, prev_time = self.cache.get_prev_running_value_state_and_time(self, field)

        # job might never have entered stage where the field starts updating (change from initial)
        if not prev_time:
            return prev_val

        dv = self.get_change_in_value_when_running_over(field, prev_time, t)
        return prev_val + dv


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

    JSON_FIELD_BATCH_JOB_SITE_NAME_MAP = "NODE RENAMES"
    JSON_VALUE_BATCH_JOB_SITE_NAME_MAP_DEFAULT = {
        "cabinet.*t2\.ucsd\.edu": "UCSDT2",
        "comet.*": "COMET"
    }

    JSON_FIELD_INIT_VALUES = "INITIAL JOB VALUES"
    JSON_VALUE_INIT_VALUES_DEFAULT = {
        Ad.remote_user_cpu_duration: (0, Job.Status.String.RUNNING, Ad.first_run_start_time),
        Ad.remote_sys_cpu_duration: (0, Job.Status.String.RUNNING, Ad.first_run_start_time)

    }

    JSON_FIELD_INFLUX_USERNAME = "INFLUX USERNAME"
    JSON_VALUE_INFLUX_USERNAME_DEFAULT = "admin"
    JSON_FIELD_INFLUX_PASSWORD = "INFLUX PASSWORD"
    JSON_VALUE_INFLUX_PASSWORD_DEFAULT = "(this isn't the real password)"

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
            self.node_renames = j[Config.JSON_FIELD_BATCH_JOB_SITE_NAME_MAP]
            self.influx_username = j[Config.JSON_FIELD_INFLUX_USERNAME]
            self.influx_password = j[Config.JSON_FIELD_INFLUX_PASSWORD]

        except IOError:
            self.bin_duration = Config.JSON_VALUE_BIN_DURATION_DEFAULT
            self.database_url = Config.JSON_VALUE_DATABASE_URL_EMPTY
            self.initial_values = Config.JSON_VALUE_INIT_VALUES_DEFAULT
            self.collector_address = Config.JSON_VALUE_COLLECTOR_ADDRESS_LOCAL
            self.constraint = Config.JSON_VALUE_JOB_CONSTRAINT_DEFAULT
            self.node_renames = Config.JSON_VALUE_BATCH_JOB_SITE_NAME_MAP_DEFAULT
            self.influx_username = Config.JSON_VALUE_INFLUX_USERNAME_DEFAULT
            self.influx_password = Config.JSON_VALUE_INFLUX_PASSWORD_DEFAULT
            obj = {
                Config.JSON_FIELD_BIN_DURATION: self.bin_duration,
                Config.JSON_FIELD_DATABASE_URL: self.database_url,
                Config.JSON_FIELD_INIT_VALUES: self.initial_values,
                Config.JSON_FIELD_COLLECTOR_ADDRESS: self.collector_address,
                Config.JSON_FIELD_JOB_CONSTRAINT: self.constraint,
                Config.JSON_FIELD_BATCH_JOB_SITE_NAME_MAP: self.node_renames,
                Config.JSON_FIELD_INFLUX_USERNAME: self.influx_username,
                Config.JSON_FIELD_INFLUX_PASSWORD: self.influx_password
            }
            FileManager.write_json_to_file(obj, FileManager.FN_CONFIG)

        # notify and exit if daemon needs configuration
        if self.database_url == Config.JSON_VALUE_DATABASE_URL_EMPTY:
            print ("Please configure the daemon (edit %s) " % FileManager.FN_CONFIG +
                   "and specify the URL at which the influx databases reside (in %s)." % Config.JSON_FIELD_DATABASE_URL +
                   "\nExiting...")
            exit()
        if self.influx_password == Config.JSON_VALUE_INFLUX_PASSWORD_DEFAULT:
            print ("Please configure the daemon (edit %s) " % FileManager.FN_CONFIG +
                   "and specify the password of the influx account for user: %s.\nExiting..." % self.influx_username)
            exit()

        # let's precompile the node rename regex
        new_dict = {}
        for regex in self.node_renames:
            new_dict[re.compile(regex)] = self.node_renames[regex]
        self.node_renames = new_dict


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

        self.schedd_ads = collector.locateAll(htcondor.DaemonTypes.Schedd)

        self.config = config
        self.constraint = config.constraint
        self.current_time = int(time.time())  # updated once jobs are requested (may use server_time from condor_q)

    @staticmethod
    def _get_all_required_fields(desired_fields):
        """
        wanting some fields may require others (e.g. for classad tinkering). This method returns a list of
        all required fields for a job, given a list of those desired by metric specifiers
        """

        required = list(Job.required_fields) + list(desired_fields)

        # the batch system uses a MockAd for the job site
        if MockAd.batch_job_site in required:
            required.remove(MockAd.batch_job_site)
            if Ad.remote_host not in required:
                required.append(Ad.remote_host)
            if Ad.last_remote_host not in required:
                required.append(Ad.last_remote_host)

        # the correct classad field is actually injected into the classad
        if MockAd.batch_submit_site in required:
            required.remove(MockAd.batch_submit_site)

        # job site is 'Unknown' when it runs on the brick, requiring default to submit site
        if (Ad.job_site in desired_fields) and (Ad.submit_site not in desired_fields):
            required.append(Ad.submit_site)

        # TODO: get fields required for fixing checkpoint dependent fields (e.g. ckpt time)

        return required

    def get_jobs(self, cache, desired_fields):
        """
        grabs all active condor jobs and those which ended since the daemon last run, which satisfy the config
        constraint, that are known to every schedd known by the config collector. Returns a list of (unique) Job
        instances with a classad containing (if present in the condor classad) the fields specified in
        desired_fields (a list of classad field strings).
        """

        required_fields = Condor._get_all_required_fields(desired_fields)

        debug_print("The metrics desire fields...\n%s\nwhich means we ask condor for fields...\n%s" % (
                    desired_fields,
                    required_fields))

        history_constraint = "((%s) && (EnteredCurrentStatus > %s))" % (self.constraint, cache.first_bin_start_time)

        debug_print("Querying schedds with constraint '%s'" % self.constraint)

        # we want unique jobs (no double counting)
        jobs = {}
        for schedd_ad in self.schedd_ads:

            schedd = htcondor.Schedd(schedd_ad)

            for ad in schedd.xquery(self.constraint, required_fields):
                job = Job(ad, cache, self.config)
                jobs[job.id] = job
                self.current_time = job.server_time

                # inject BATCH_SUBMIT_SITE if required
                if MockAd.batch_submit_site in desired_fields:
                    job.ad[MockAd.batch_submit_site] = schedd_ad["Machine"]

            for ad in schedd.history(history_constraint, required_fields, 10000):
                job = Job(ad, cache, self.config)
                jobs[job.id] = job

                # inject BATCH_SUBMIT_SITE if required
                if MockAd.batch_submit_site in desired_fields:
                    job.ad[MockAd.batch_submit_site] = schedd_ad["Machine"]

        return [jobs[id] for id in jobs]


class MetricManager(object):

    DEFAULT_METRICS = '''
#!/usr/bin/env python

# Author:       Tyson Jones, January 2016 (MURPA student of Prof Frank Wuerthwein, UCSD).
#               Feel free to contact me at  tjon14@student.monash.edu

# Purpose:      user specified metrics for the condorflux system


"""
For your reference...

--------------------------------------------------------------------------------------
mock ads (additional classad_tags)...

BATCH_SUBMIT_SITE:      uses Schedd name from which job was collected
BATCH_JOB_SITE:         uses LastRemoteHost (after first @), though is
                        replaced by a name in Config `NODE RENAMES` if
                        it matches a regex therein.
--------------------------------------------------------------------------------------
time bin attributes...

start_time
end_time

time bin methods...

add_to_sum(val, tags)
add_to_job_average(val, tags)
add_to_time_average(val, tags, duration)
add_to_division_of_sums(num, den, tags)

get_sum()
get_job_average()
get_time_average()
get_division_of_sums()
--------------------------------------------------------------------------------------
job attributes...

ad                              - the job's classad, used for grabbing condor values.
                                  e.g. job.ad['SUBMIT_SITE']

job methods...

get_values(fields)              - given a list of classad fields (or mock ads),
                                  returns {field: value} with the job's corresponding
                                  values

is_idle()                       - returns whether the job is currently idle
is_running()
is_removed()
is_completed()
is_held()
is_transferring_output()
is_active()

was_idle()                      - returns whether the job's very previous state was idle
was_running()
was_held()
was_transferring_output()

get_most_recent_time_span_idle()    - returns (start, end) of the job's most recent
                                      time being idle. If job is still idle, end=False
get_most_recent_time_span_running() - [as above]. If job has never run, start=False

is_idle_during(t0, t1)          - returns job was ever in the idle state within [t0, t1]
is_running_during(t0, t1)

get_time_idle_in(t0, t1)        - returns duration for which job is idle in [t0, t1]
get_time_running_in(t0, t1)

get_rate_of_change_of_value_when_running(field)
get_change_in_value_when_running_over(field, t0, t1)
get_value_when_running_at(field, t)
--------------------------------------------------------------------------------------
"""



def count_idle_jobs(self, time_bin, jobs):
    for job in jobs:
        if job.is_idle_during(time_bin.start_time, time_bin.end_time):
            time_bin.add_to_sum(1, job.get_values(self.tags))
    return time_bin.get_sum()

def count_running_jobs(self, time_bin, jobs):
    for job in jobs:
        if job.is_running_during(time_bin.start_time, time_bin.end_time):
            time_bin.add_to_sum(1, job.get_values(self.tags))
    return time_bin.get_sum()


"""
    attributes:
        db               - name of the influx DB (created if doesn't exist)
        mes              - measurement name with which to label metric in DB
        tags             - list of classad fields (or mock ads) which will
                           segregate values at a time for this metric, becoming
                           tags in the influxDB measurement
        fields           - any additional job classad fields that this metric will
                           look at (e.g. for metric value calculation).
                           These must be declared so that the daemon can fetch any
                           needed classads from condor
        cache            - any job classad fields which should be cached by the daemon.
                           caching is required when a change in a field is required,
                           or its value at a particular (non current) time is sought
                           (i.e. when interpolation is required)
                           This should be a subset of fields, though the daemon will forgive
                           you if you forgot to put any fields needed to be cache in fields
                           (it will add them)
"""


class RunningPerSitesMetric:
    db = "GlideInMetrics"
    mes = "running jobs"
    tags = ["SUBMIT_SITE", "MATCH_EXP_JOB_Site"]
    fields = []
    cache = []
    calculate_at_bin = count_running_jobs

class RunningPerOwnerAndSubmitSiteMetric:
    db = "GlideInMetrics"
    mes = "running jobs"
    tags = ["SUBMIT_SITE", "Owner"]
    fields = []
    cache = []
    calculate_at_bin = count_running_jobs

class IdlePerOwnerAndSubmitMetric:
    db = "GlideInMetrics"
    mes = "idle jobs"
    tags = ["SUBMIT_SITE", "Owner"]
    fields = []
    cache = []
    calculate_at_bin = count_idle_jobs

class IdlePerSubmitMetric:
    db = "GlideInMetrics"
    mes = "idle jobs"
    tags = ["SUBMIT_SITE"]
    fields = []
    cache = []
    calculate_at_bin = count_idle_jobs

'''

    def __init__(self):

        self.metrics = []

        # try to load metrics from file
        try:
            sys.dont_write_bytecode = True
            module_name = FileManager.FN_METRICS.split('.')[0]
            metrics = __import__(module_name)

            # grab all classes declared in the metrics file
            for _, obj in inspect.getmembers(metrics):
                if inspect.isclass(obj) and (obj.__module__ == module_name):
                    self.metrics.append(obj)

        # otherwise create a default metrics spec file
        except ImportError as e:
            print ("Creating file %s with default contents. " % FileManager.FN_METRICS +
                   "Please edit this to specify custom metrics. Note that the default metrics aren't loaded " +
                   "for this daemon's execution; proceeding with no metric collection.")
            FileManager.write_str_to_file(MetricManager.DEFAULT_METRICS, FileManager.FN_METRICS)

    def get_all_desired_fields(self):
        """returns a list of all desired classad fields or mockads in the user specified metrics"""
        fields = set()
        for metric in self.metrics:
            for field in metric.tags:
                fields.add(field)
            for field in metric.fields:
                fields.add(field)
            for field in metric.cache:
                fields.add(field)

        return list(fields)

    def process_metrics(self, bin_times, bin_duration, jobs, outbox):

        for metric_class in self.metrics:

            metric_inst = metric_class()
            debug_print("Processing metric: %s %s" % (metric_inst.mes, '(' + ', '.join(metric_inst.tags) + ')'))

            # filter for only jobs which contain the fields the metric needs
            valid_jobs = []
            for job in jobs:
                try:
                    job.get_values(metric_inst.tags)
                    job.get_values(metric_inst.fields)
                    job.get_values(metric_inst.cache)
                    valid_jobs.append(job)
                except RuntimeError as e:
                    debug_print("The following job was excluded from this metric (the metric " +
                                "needed fields %s, some of which weren't present)" % (
                                    metric_inst.tags + metric_inst.fields))
                    debug_print(prettify(job.ad))
                    debug_print("The caught error reads:\n%s" % str(e))
                    continue

            # calculate the metric at each time bin using only filtered jobs
            for t in bin_times:
                time_bin = Bin(t, t + bin_duration)
                results = metric_inst.calculate_at_bin(time_bin, valid_jobs)
                outbox.add(metric_inst.db, metric_inst.mes, results, time_bin.start_time)

            debug_print("At the final bin, metric %s yielded %s" % (metric_inst.mes, prettify(results)))

    def are_no_metrics(self):
        return not len(self.metrics)

    def get_fields_to_cache(self):
        """returns a list of clasad fields to cache for each job"""
        fields_to_cache = set()
        for metric_class in self.metrics:
            for field in metric_class.cache:
                fields_to_cache.add(field)
        return list(fields_to_cache)


def debug_print(msg):
    """prints msg only if the daemon is in debug mode (DEBUG_PRINT is True)"""
    if DEBUG_PRINT:
        print msg


def prettify(object):
    """encodes an object as a string as prettily as it can"""
    try:
        return json.dumps(object, indent=4)
    except TypeError:
        return str(object)


def main():

    # load contextual files
    metricmngr = MetricManager()
    config = Config()
    cache = Cache(config)
    condor = Condor(config)
    outbox = Outbox(config)

    # let's exit early (note we're dodging caching) if there's no metrics to collect
    if metricmngr.are_no_metrics():
        print "There are zero specified metrics. Exiting."
        exit()

    # get jobs
    jobs = condor.get_jobs(cache, metricmngr.get_all_desired_fields())

    # allocate time since previous run into bins
    bin_times = range(cache.first_bin_start_time, condor.current_time, config.bin_duration)
    if len(bin_times) < 2:
        print ("The daemon has been run too recently at %s; no bins (duration %s) have transpired" % (
            cache.first_bin_start_time, config.bin_duration))
        exit()
    bin_start_times, final_bin_end_time = bin_times[:-1], bin_times[-1]
    del bin_times

    # calc every metric at every bin and add results to the outbox
    metricmngr.process_metrics(bin_start_times, config.bin_duration, jobs, outbox)

    # push outbox to influx
    outbox.push_outgoing()
    outbox.save()

    # cache any required fields
    Cache.save_time_and_running_values(final_bin_end_time, jobs, metricmngr.get_fields_to_cache())

main()