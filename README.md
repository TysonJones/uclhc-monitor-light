
Setting up Condorflux
=================
> Tyson Jones
> tjon14@student.monash.edu
> MURPA student at UCSD, February 2016
> Under the supervision Frank Wuerthwein, Jeffrey Dost and Edgar Fernandez

The Condorflux system combines Telegraf, InfluxDB, Grafana and the Condor python bindings to probe Condor, collect usage metrics and display them.

-------------------------------------

[InfluxDB](https://docs.influxdata.com/influxdb/v0.10/introduction/installation/)   
----------

> Influx runs on the front-end.

###<i class="icon-download"> Download and Install</i>

```
cat <<EOF | sudo tee /etc/yum.repos.d/influxdb.repo
[influxdb]
name = InfluxDB Repository - RHEL \$releasever
baseurl = https://repos.influxdata.com/rhel/\$releasever/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdb.key
EOF
```

```
sudo yum install influxdb
```
###<i class="icon-play">Start InfluxDB</i> 
```
sudo service influxdb start
```
Test you can access the server by visiting the admin panel at 
```
http://[your-domain]:8083/
```
 If unreachable, you may need to...

###<i class="icon-wrench"> Configure your firewall</i>

```
nano /etc/sysconfig/iptables
```
Add the following rules (these may be publicly accessible since we'll enable InfluxDB authentication)
```
# InfluxDB admin panel
-A INPUT -m state --state NEW -m tcp -p tcp --dport 8083 -j ACCEPT
# InfluxDB http GET/POSTs
-A INPUT -m state --state NEW -m tcp -p tcp --dport 8086 -j ACCEPT
# Grafana webpage
-A INPUT -m state --state NEW -m tcp -p tcp --dport 3000 -j ACCEPT
```
then
```
service iptables reload
service iptables start
```

### <i class="icon-plus"> Create an Admin User</i>
Enter the InfluxDB shell
```
influx
>
```
Create a new username and password which must be supplied to query any databases through the HTTP interface. 
Here, we make username: **admin** with password: **correct horse battery staple**
```
> CREATE USER admin WITH PASSWORD 'correcthorsebatterystaple' WITH ALL PRIVILEGES
```
```
> exit
```

### <i class="icon-lock"> Enable Authentication</i>

Edit the default InfluxDB configuration
```
nano /etc/influxdb/influxdb.conf
```
Navigate to the `[http]` section, and adjust
```
auth-enabled = true
```
> The default config can be regenerated via 
> ```
> influxd config > /etc/influxdb/influxdb.conf
> ```

> After enabling, use of the InfluxDB shell will require authentication.
> ```
> influx
> > auth admin correcthorsebatterystaple
> ```

###<i class="icon-cw"> Restart InfluxDB</i>

``` 
sudo service influxdb restart
```

### <i class="icon-ok"> Test InfluxDB </i>

Create a database `testdb` through the HTTP interface, passing your credentials.
```
http://[your-domain]:8086/query?q=CREATE%20DATABASE%20testdb&u=admin&p=correcthorsebatterystaple
```
If successful, this should yield response 
```
{"results":[{}]}
```
Let's spoof some data we can see in Grafana later.
Open
```
http://[your-domain]:8083/
```
and enter your username and password (**admin** and **correcthorsebatterystaple**).

Ensure you're using the <kbd>testdb</kbd> database in the top-right corner.
Select <kbd>Write Data</kbd> and submit
```
testmes value=1 1455824000000000000
testmes value=3 1455824100000000000
testmes value=2 1455824200000000000
testmes value=4 1455824300000000000
```
(these nanosecond since epoch timestamps correspond to `18 Feb 2016`, `11:33am`, `GMT-8:00`)

We'll see these later, though you can check they were written now by entering the query
```
SELECT * FROM testmes
```
and hitting <kbd>Enter</kbd>




> log located at `/var/log/influxdb/influxd.log`


###<i class="icon-cog"> Setup Data Retention </i>

See [here](https://docs.influxdata.com/influxdb/v0.10/)
_____________________________________________________________________________________

[Grafana](http://grafana.org/)
----------

> Grafana should run on the front end, though it is not necessary that it run on the same machine as Influx

###<i class="icon-download"> Download and Install</i>

```
cat <<EOF | sudo tee /etc/yum.repos.d/grafana.repo
[grafana]
name=grafana
baseurl=https://packagecloud.io/grafana/stable/el/6/$basearch
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://packagecloud.io/gpg.key https://grafanarel.s3.amazonaws.com/RPM-GPG-KEY-grafana
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF
```

```
sudo yum install grafana
```

If this fails, install version 2.6.0 directly.
```
sudo yum -y install  https://grafanarel.s3.amazonaws.com/builds/grafana-2.6.0-1.x86_64.rpm
```

###<i class="icon-play">Start Grafana</i>
```
sudo service grafana-server start
```


###<i class="icon-wrench">Configure Grafana </i>
```
nano /etc/grafana/grafana.ini
```
Navigate to the `Server` section, uncomment (remove the `;`) `domain` and replace `localhost` with your domain.
```
domain = [your-domain]
```
Navigate to the `Anonymous Auth` section and set
```
enabled = true
org_name = [your organization name]
org_role = Viewer
```
`[your organization name]` should be that which you'll create in the proceeding section.

Navigate to the `SMTP / Emailing` section and set
```
enabled = true
host = [your-domain]:25
from_address = admin@condorflux.[your-domain]
```


###<i class="icon-cw">Restart Grafana</i>
```
sudo service grafana-server restart
```

###<i class="icon-plus">Create a User</i>

Visit
```
http://[your-domain]:3000
```
and select <kbd>Sign up</kbd>, then enter your details (*may require email confirmation*).

###<i class="icon-plus">Create an Organization</i>
A Grafana organization groups users and the dashboards they can view.


- View the side menubar by pressing the top left button.

- In the side menubar, click <kbd>Main Org.</kbd>, select <kbd>+ New Organization</kbd> and enter your organization's name (e.g. **UCSD**)

###<i class="icon-mail"> Invite Users</i>

In the side menubar, click your organization, select <kbd>Users</kbd> then <kbd> + Add or inivite </kbd>, and enter their details.

> If a SMTP server is running on port  `25`, this will send an invitation email.
If not, no error will be reported but no email will be sent. You can manually send the email by visiting <kbd>Pending Invitations</kbd>, clicking on <kbd>Details</kbd> of the invitation and copying the address (e.g. http://[your-domain]:300/invite/abcdefg) into an email you deliver yourself.

###<i class="icon-ok"> Test Grafana</i>

- Click <kbd>Data Sources</kbd> from the side menubar, then <kbd>Add new </kb> at the top right.

- Enter
  `Name`:  `test source`
  `Type`: `InfluxDB 0.9.x`
  `Url`:  `http://[your-domain]:8086`
  `Access`: `direct`
  `Basic Auth`: `[unchecked]`
  `With Credentials`: `[unchecked]`
  `Database`: `testdb`
  `User`: `admin`
  `Password`: `correcthorsebatterystaple`

  and click <kbd>Test Connection</kbd>, then <kbd>Save</kbd>


- Click <kbd>Dashboards</kbd> in the side menubar, <kbd>Home</kbd> in the top left and select <kbd>+ New</kbd> in the dropdown menu to create a new dashboard.

- Click the green rectangle, and select <kbd>Add Panel</kbd> > <kbd>Graph</kbd>

- Under the <kbd>Metrics</kbd> tab, set the bottom-right dropdown box to `test source`, then press <kbd>+ Query</kbd>

- Adjust the query to be
  ```
  FROM testmas SELECT field(value)
   ```
   and the time display (top right corner) to be `18 Feb 2016` `11:30am` to `18 Feb 2016` `11:40am`, and the spoof data should be visible.

> If you're satisfied with your tests, discard your test graph by navigating away (e.g. clicking on <kbd>Dashboards</kbd> in the side menubar) and selecting <kbd>Ignore</kbd> in the `Unsaved changes!` alert.

> Discard your test database with the query
> ```
> http://[your-domain]:8086/query?q=DROP%20DATABASE%20testdb&u=admin&p=correcthorsebatterystaple
> ```

--------------------------------------------------------------------

[Telegraf](https://influxdata.com/time-series-platform/telegraf/)
---------

> Telegraf runs on each machine of which to monitor the system


--------------------------------------------------------------------

Condorflux Daemon
-----------------------

> The daemon runs on each GlideInWMS submit site, or any machine which can query a Condor collector

###<i class="icon-download"> Place the Daemon</i>

The daemon requires no special priveleges or location to run, though must make and edit files in its working directory.

###<i class="icon-play"> Run the Daemon</i>

```
python daemon.py
```
The daemon should report a missing metrics file and a configuration error and exit, though has now created a default configuration file and metrics file.


###<i class="icon-wrench"> Configure the Daemon</i>
```
nano config.json
```
Update the follow fields
```
"INFLUX USERNAME": "admin",
"INFLUX PASSWORD": "correcthorsebatterystaple",
"DATABASE URL": "http://[your-domain]:8086",
```
> Note that if keeping this username and password private is important, then keep in mind they're in the URLs opened by the daemon which are printed in debug mode (`DEBUG_MODE = True`), which may be logged by the CRON job, so make the log private.

You may optionally change the field `JOB CONSTRAINT` which constrains which jobs are collected from the Condor binaries, in the [Condor expression syntax](http://research.cs.wisc.edu/htcondor/manual/v7.6/4_1Condor_s_ClassAd.html). Note classad string literals must be in quotations, which must be escaped to be valid JSON. For example
```
"JOB CONSTRAINT": "Owner=?=\"jdost\""
```

If the daemon is to use a non-local collector (e.g. it is running on a gateway though collects on behalf of other gateways under the same collector), then the `COLLECTOR ADDRESS` should also be changed. For example
```
"COLLECTOR ADDRESS": "osg-gw-1.t2.ucsd.edu",
```
Note also if you intend to group and tag metrics by `BATCH_JOB_SITE`, which employs the `LastRemoteHost` of the job, you should fill `"NODE RENAMES"` with `"[regex]": "[name]"` pairs. If a job's host matches a regex, it will use the associated name in place of the host name, and aggregate with other metrics of the same name.
For example,
```
"NODE RENAMES": {
    ".*\\.t2\\.ucsd\\.edu": "UCSD"
}
```
will result in jobs with `LastRemoteHosts` of `cabinet-0-0-1.t2.ucsd.edu`, `cabinet-5-5-5.t2.ucsd.edu` and `cabinet-8-8-4.t2.ucsd.edu` all being treated as running on the same `BATCH_JOB_SITE` of `UCSD`.

###<i class="icon-plus"> Add Metrics</i>

Please see the proceeding section

###<i class="icon-cog"> Setup a CRON</i>

For example
```
crontab -e
```
```
*/15 * * * * ./cron.sh
```
where `cron.sh` reads as
```
cd /path/to/daemon/directory
python daemon.py
```


-----------------------------------------------------

Creating Custom Metrics
====================

> Creating a custom metric involves editing the **daemon**'s auxiliary file `metrics.py` (to have the daemon collect and push the metrics to influx) and adding a **Grafana** graph (to pull metrics from influx and display them).

###<i class="icon-info"> Daemon Operation </i>

The daemon remembers the last time it run, and only ever considers jobs which are currently running, or started/ended since the daemon last run (this includes jobs which started then ended between daemon runs). The daemon splits the time since the previous and now into configurably sized time bins (the final bin may end before the current time, if the time since the previous run is not even divisible by the bin size), and metrics are calculated at each bin. Custom metrics specify how they're to be calculated for a time bin.

The daemon wraps classads in a Job object with many convenient methods, and provides a Bin object which abstracts the tedium of separating metric calculations between tagged values. These are elaborated upon a few sections down.

###<i class="icon-plus"> Create Metric in Daemon </i>

Custom metrics are specified in `metrics.py` (in the same directory as `daemon.py`) as a class (of an *arbitrary*, but requiredly *unique* class name) with attributes `db`, `mes`, `tags`, `fields` and a non-static method `calculate_at_bin(time_bin, jobs)`.

####db
- The name of the influxDB in which to store this metric.
- This can be an existing or an original (will be automatically created) database. *If this is a new database, it will need to be manually added to Grafana as a datasource later, and its exact name is required memorising.*

####mes
 - The prefix of the influxDB measurement name of this metric; this is suffixed with the metrics `tags` list comma-separated in the full measurement name.  This identifier is used when adding this metric to Grafana graphs and is discoverable there.
 -  Different daemon instances (i.e. running on different machines) will contribute to the same metric if they share the same `mes` and `tags` (though `tags` must include something site specific, like `SUBMIT_SITE`, to ensure the daemon's don't overwrite eachother's data; see *PITFALLS*)

####tags
- A list of Condor classad fields by which to split the metric's aggregation, which become InfluxDB tags with the job's classad's values. That is, jobs with differing classad values (corresponding to the fields in `tags`) will be included in separate calculations of the metric.
- *The list may also include special fields "BATCH_JOB_SITE" and "BATCH_SUBMIT_SITE" which aren't actual condor classad fields, but are transformed to correspond to the sites of where the job ran and was submitted in the BATCH system*.
- If a job's classad doesn't include all fields in `tags` (which aren't a *"special field"*), it won't be passed to the metric / included in its calculation.

####fields
- A list of any additional Condor classad fields the metric needs for its calculation. This must be declared so that the stripped classads passed to the metric's `calculate_at_bin` method actually contain the needed fields.
- If a job doesn't contain the declared fields, it will be excluded from the list passed to `calculate_at_bin`.

####calculate_at_bin
- A non-static method called by the daemon to calculate the metric at a particular time bin.
- The time bin is passed as a `Bin` object. Also passed is a list of all jobs (as `Job` objects) which contain all fields in the metric's `fields` and `tags` in the job's classad (`Job.ad`).
  **E.g.** if `tags = ["Owner"]` and `fields = ["DiskUsage", "RemoteUserCpu"]`, then any job known by the daemon which doesn't contain all of *"Owner"*, *"DiskUsage"* and *"RemoteUserCpu"* in its classad will be excluded from the `jobs` passed to `calculate_at_bin`.

_____________________________________
For example,
```
class ExampleMetric:
    db = "example metric database"
    mes = "num running jobs"
    tags = ["SUBMIT_SITE", "MATCH_EXP_JOB_Site"],
    fields = []

    def calculate_at_bin(self, time_bin, jobs):
        for job in jobs:
            if job.is_running_during(time_bin.start_time, time_bin.end_time):
                time_bin.add_to_sum(1, job.get_values(self.tags))
        return time_bin.get_sum()
```
might result in a push to influxDB *"example metric database"* for time *"12345"* with body
```
num\ running\ jobs,SUBMIT_SITE=UCSD,MATCH_EXP_JOB_SITE=UCI value=10 12345
num\ running\ jobs,SUBMIT_SITE=UCI,MATCH_EXP_JOB_SITE=UCI value=2 12345
num\ running\ jobs,SUBMIT_SITE=UCSD,MATCH_EXP_JOB_SITE=UCR value=14 12345
num\ running\ jobs,SUBMIT_SITE=UCI,MATCH_EXP_JOB_SITE=UCR value=1 12345
...
```
_______________________________

For convenience, when performing the same calculation (e.g. counting idle jobs) in multiple metrics, the operation function can be specified globally and attached to a metric.

For example, `metrics.py` could read
```
def count_idle_jobs(self, time_bin, jobs):
    for job in jobs:
        if job.is_idle_during(time_bin.start_time, time_bin.end_time):
            time_bin.add_to_sum(1, job.get_values(self.tags))
    return time_bin.get_sum()

class IdlePerOwnerAndSubmitMetric:
    db = "GlideInMetrics"
    mes = "idle jobs"
    tags = ["SUBMIT_SITE", "Owner"]
    fields = []
    calculate_at_bin = count_idle_jobs

class TotalIdlePerSubmitMetric:
    db = "GlideInMetrics"
    mes = "idle jobs"
    tags = ["SUBMIT_SITE"]
    fields = []
    calculate_at_bin = count_idle_jobs
```
`metrics.py` can be extended with custom functions and use external libraries, but must **not** define any classes which aren't to be interpreted as metrics.

Metrics are most easily temporarily removed by "commenting" them out as a multiline string.
```
"""

class Metric:
...

"""
```




### PITFALLS

> Different daemon instances on different submit sites may overwrite each other's measurement data if not tagged by some unique feature, like `SUBMIT_SITE` or `Owner` (when site specific).
> For example, a metric of the same name counting running jobs tagged by `MATCH_EXP_JOB_Site` which is running on both the *UCSD* and *UCI* submit sites will each have access only to a subset of the total job pool (jobs which were submitted from them), and each under-report how many jobs are running at a particular site. To combat this, tag by `SUBMIT_SITE` and if non-submit-site-specific data is required, aggregate in Grafana.

_________________________
When collecting information about a job's status (i.e. compiling the number of running jobs at a time), only the job's latest instance of being in that state is considered. I.e. if the daemon is stopped and the job runs, idles then runs, then when the daemon is resumed thereafter, it will only collect information about the latest run state (after the latest idle). This is due to an indeterminacy in the time span of the previous run state, due to the overwriting of condor classads.
(using JobStartDate, JobLastStartDate and JobCurrentStartDate, some more instances can be learned but this is not worth the effort; just keep the daemon running frequently and accept that huge backward-in-time info grabbing offers limited accuracy)
___________________________


###<i class="icon-plus"> Add Datasource to Grafana </i>

> optional (may be used in another one)

> Select Influx0.9, even though we're using 0.10


###<i class="icon-plus"> Add Graph to Grafana </i>

> select the corresponding database

> select the corresponding measurement name

> If stack, set to `individual`

> do staircase

---------------------------------------------------------------------
INTEGRATE THIS
_______________________________

`SELECT` `field(value)` `mean()`
`GROUP BY` `time(15m)` `fill(0)`

`time()` should contain a duration **equal to** (preferred) or **larger than** (preferably a multiple of) the **duration between daemon runs**.

`fill(0)` is used to replace null values at times (there were no timestamps sent to database within the time interval) with zero. This will give dud zeroes if `time()` doesn't contains too small a duration (less than duration between daemon runs)

When displaying specifically integer information (not to be interpolated) like the number of jobs running, ensure `staircase line` in `Display Styles` is ticked (interpolation should only erroneously occur if grafana's interval grouping is misaligned to the daemons bins, which is uncontrollable. I think Grafana will align if it can).

_________________________________

Editing / Deleting Custom Metrics
====================

Solely changing the daemon's `metrics.py` will leave old measurements in the front-end databases, the lack of re-population of which may be confused as actual data. Measurements which are no longer being updated should be removed from the databases by...

> Remove from metrics.py

> Remove measurements from influxDB
> (url query)

> Remove from any grafana graphs

> (Optional) Delete database, datasource, dashboard



____________________________________________________

Daemon Object Documentation
=========================

Job
----


Bin
----