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
--------------------------------------------------------------------------------------
"""


class Metric(object):

    def __init__(self, database_name, measurement_name, classad_tags, classad_fields=[]):
        """
        A specification of a metric.

        Arguments:
            database_name    - name of the influx DB (created if doesn't exist)
            measurement_name - measurement name with which to label metric in DB
            classad_tags     - list of classad fields (or mock ads) which will
                               segregate values at a time for this metric, becoming
                               tags in the influxDB measurement
            classad_fields   - any additional job classad fields that this metric will
                               look at (e.g. for metric value calculation).
                               These must be declared so that the daemon can fetch any
                               needed classads from condor
        """
        self.db = database_name
        self.mes = measurement_name
        self.tags = classad_tags
        self.fields = classad_fields

    def calculate_at_bin(self, time_bin, jobs):
        """
        """

        raise ReferenceError("")


"""
--------------------------------------------------------------------------------------
"""


class metric0(Metric):
    def __init__(self):

        db = 'testdb'
        mes = 'testmes0'
        tags = ['Owner']
        fields = []
        super(metric0, self).__init__(db, mes, tags, fields)

    def calculate_at_bin(self, time_bin, jobs):
        
        for job in jobs:
            if job.is_idle_during(time_bin.start_time, time_bin.end_time):
                time_bin.add_to_sum(1, job.get_values(self.tags))
        return time_bin.get_sum()

            
class metric1(Metric):
    def __init__(self):

        db = 'testdb'
        mes = 'testmes1'
        tags = ['SUBMIT_SITE']
        fields = ['DiskUsage']
        super(metric1, self).__init__(db, mes, tags, fields)

    def calculate_at_bin(self, time_bin, jobs):

        # dumb/meaningless calc
        # job-runtime-in-bin weighted average of total DiskUsage
        
        for job in jobs:
            if job.is_running_during(time_bin.start_time, time_bin.end_time):
                time_bin.add_to_time_average(
                    job.ad["DiskUsage"],
                    job.get_values(self.tags),
                    job.get_time_running_in(
                        time_bin.start_time,
                        tim_bin.end_time
                    )
                )
        return time_bin.get_time_average()

            



