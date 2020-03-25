# Changelog

## version 0.2.4
- Fixed bug with number of used CPUs *sigh*

## version 0.2.3
- Fixed bug with number of used CPUs

## version 0.2.2
- Get number of used CPUs from database
- Default ncpus to 1 and job name to filename of script

## version 0.2.1
- Added "nodename" option for configuration file
- Don't "free" CPUs on deleting jobs, if job is not actually running

## version 0.2
- Added support for "-o" (priority) parameter in header
- Added new commands "config" and "set" to fetch and set config parameters
- Tweaked statistics
- Fixed bug with freeing CPUs after removing job
