# PyBS - The Python Batch System

## Table of Contents
* [Installation](#installation)
    * [PyBS](#pybs)
    * [Database](#database)
    * [Configuration](#configuration)
    * [systemd](#systemd)
* [Usage](#usage)
    * [Aliases](#aliases)
    * [Submitting a job](#submitting-a-job)
    * [Deleting a job](#deleting-a-job)
    * [Job list](#job-list)
    * [Start a waiting job](#start-a-waiting-job)

## Installation

### PyBS

The easiest way for installing *PyBS* is by cloning its repository and running setup.py:

    git clone git@gitlab.gwdg.de:thusser/pybs.git
    cd pybs
    sudo pip3 install -r requirements.txt
    sudo python3 setup.py install

### User

The safest way for using a batch system like *PyBS* is by running it under its own system user, which can be 
created via:

    sudo useradd --user-group --home-dir /home/pybs --create-home --password <password> pybs

### Database

*PyBS* requires a database for storing the list of jobs. Supported are all types of databases that
[SQLAlchemy](https://www.sqlalchemy.org/) can handle, e.g. MySQL and sqlite.

When using a database system like MySQL, we need to create a database and a user with access to it. The following
example creates a database `pybs` and the user `pybs` (with password "pybs") with full access:

    # create database
    create database pybs;
   
    # create user
    create user pybs;
    
    # grant privileges and set password
    grant all on pybs.* to ‘pybs’@’localhost’ identified by ‘pybs’;

### Configuration

The default location for the configuration is /etc/pybs, but you can change that with the `--config` parameter
when calling `pybsd`. A typical configuration looks like this:

    # Maximum number of CPU cores to use
    ncpus      = 8
    
    # Database connection
    # MySQL:
    #   mysql://<user>:<password>@<hostname>:<port>/<database>
    #   E.g.: mysql://pybs:pybs@localhost:3306/pybs
    # sqlite:
    #   sqlite:///path/to/database.db
    #   E.g.: sqlite:///home/pybs/pybs.db
    database    = sqlite:////home/pybs/pybs.db
    
    # Root directory
    # The root directory is only important on multi-node systems, i.e. running on different machines. The script
    # to run must be available on all systems, but can be mounted into different directories. If, for instance,
    # it is located at /mountA/jobs/script.sh on machine A, and at /mountB/some_directory/jobs/script.sh on
    # machine B, then root could be set to /mountA on machine A, and /mountB/some_directory on machine B.
    root        = /

### systemd

In order to start PyBS as a service, create a systemd configuration file /usr/lib/systemd/system/pybs.service:

    [Unit]
    Description=GCDB Batch System daemon
    After=syslog.target
    
    [Service]
    Type=simple
    User=pybs
    Group=pybs
    WorkingDirectory=/home/pybs/
    ExecStart=/usr/bin/gbsd
    StandardOutput=syslog
    StandardError=syslog
    
    [Install]
    WantedBy=multi-user.target

Now you can start/stop *PyBS* via `service pybs start/stop`.

If you need to set up a special environment (e.g. like [pyenv](https://github.com/pyenv/pyenv)), you can
create a separate script somewhere (e.g. /usr/local/bin/start_pybs.sh) with content like this:

    #!/bin/bash
    
    # set up pyenv
    export PYENV_ROOT="/opt/pyenv"
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
    pyenv global 3.5.2
    
    # start pybsd
    /opt/pyenv/shims/pybsd
    
Now change the ExecStart setting in pybs.service to this script and everything should work fine.

## Usage

After the *PyBS* daemon *pybsd* has been started, the command line interface `pybs` can be used for accessing 
the system. 

### Aliases

If you are used to a PBS system like [Torque](http://www.adaptivecomputing.com/products/torque/), you might want
to create aliases to mimic its behaviour:

    alias qsub='pybs sub'
    alias qdel='pybs del'
    alias qstat='pybs stat'
    alias qrun='pybs run'

### Submitting a job

A new job can easily be submitted to the system using the `pybs sub` (or `qsub`) command:

    pybs sub /path/to/script
    
The script must be executable and contain a PBS header with at least a name and the number of requested CPU cores:

    #PBS -N NameOfJob
    #PBS -l ncpus=4
    
Furthermore you can define files where stdout and stderr will be written to:

    #PBS -o output
    #PBS -e error
    
Please note that all pathes are relative to the path of the script itself and that the working directory of the 
script itself is always its path.

A job can be limited to one or more nodes, which can also be defined in the header as comma-separated list:

    #PBS -nodes nodeA,nodeB
    
After successfully submitting a job, its ID will be written to standard output.

### Deleting a job

A waiting or running job can be deleted via `pybs del` (or `qdel`):

    pybs del <id>
    
with the ID of the job. If the job is actually running, its process will be terminated by sending a SIGKILL signal.

### Job list

A list of waiting and running jobs is shown when calling:

    pybs stat
    
### Start a waiting job
 
 A waiting job can be started immediately, ignoring all constraints, using:
 
    pybs run <id>
