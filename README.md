# PyBS - The Python Batch System

## Table of Contents
* [Installation](#installation)
    * [PyBS](#pybs)
    * [Database](#database)
    * [Configuration](#configuration)
    * [systemd](#systemd)

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
    ncores      = 8
    
    # Database connection
    # MySQL:
    #   mysql://<user>:<password>@<hostname>:<port>/<database>
    #   E.g.: mysql://pybs:pybs@localhost:3306/pybs
    # sqlite:
    #   sqlite:///path/to/database.db
    #   E.g.: sqlite:///home/pybs/pybs.db
    database    = sqlite:///home/pybs/pybs.db
    
    # Root directory
    # The root directory is only important on multi-node systems, i.e. running on different machines. The script
    # to run must be available on all systems, but can be mounted into different directories. If, for instance,
    # it is located at /mountA/jobs/script.sh on machine A, and at /mountB/some_directory/jobs/script.sh on
    # machine B, then root could be set to /mountA on machine A, and /mountB/some_directory on machine B.
    root        = /

### systemd

In order to start PyBS as a service, create a systemd configuration file /usr/lib/systemd/system/pybs.system:

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