import datetime
import os
import re
from sqlalchemy import Column, Integer, String, DateTime

from .base import Base


class Job(Base):
    """A single job in the database."""
    __tablename__ = 'job'

    id = Column(Integer, comment='unique ID for job', primary_key=True)
    name = Column(String(100), comment='job name', index=True, nullable=False)
    username = Column(String(20), comment='submitting user', nullable=False)
    filename = Column(String(200), comment='filename of submitted script', nullable=False)
    ncpus = Column(Integer, comment='number of requested CPUs', nullable=False)
    priority = Column(Integer, comment='priority of job', nullable=False, default=0)
    nodes = Column(String(100), comment='run job only on nodes in this comma-separated list')
    node = Column(String(100), comment='node that job actually runs/ran on')
    pid = Column(Integer, comment='process ID of running job')
    submitted = Column(DateTime, comment='date and time of submission')
    started = Column(DateTime, comment='date and time of execution start')
    finished = Column(DateTime, comment='date and time of execution end')

    @staticmethod
    def parse_pbs_header(filename: str) -> dict:
        """Parse the PBS header in the file connected to this job.

        Example for a PBS header:
        #PBS -l ncpus=20
        #PBS -N {{JOBNAME}}
        #PBS -e {{PATH}}/{{NAME}}.error
        #PBS -o {{PATH}}/{{NAME}}.output
        #PBS -m a
        #PBS -M musegc@astro.physik.uni-goettingen.de

        Args:
            filename: Name of file to parse.

        Returns:
            Dictionary with all header items.
        """

        # compile regexp
        regexp = re.compile(r'#PBS \-(\w) (.*)')

        # init header
        header = {}

        # open file
        with open(filename, 'r') as f:
            for line in f:
                # apply regexp
                m = regexp.match(line)
                if m is None:
                    continue

                # fill header
                if m.group(1) == 'N':
                    header['name'] = m.group(2)
                elif m.group(1) == 'l':
                    s = m.group(2).split('=')
                    header[s[0]] = s[1]
                elif m.group(1) == 'e':
                    header['error'] = m.group(2)
                elif m.group(1) == 'o':
                    header['output'] = m.group(2)
                elif m.group(1) == 'm':
                    header['send_mail'] = m.group(2)
                elif m.group(1) == 'M':
                    header['email'] = m.group(2)
                elif m.group(1) == 'p':
                    header['priority'] = int(m.group(2))

        # return it
        return header

    @staticmethod
    def from_file(filename: str) -> 'Job':
        """Create a new Job object from a given script file.

        Args:
            filename: Name of file to parse PBS header from.

        Returns:
            New job created from file.
        """

        # file exists?
        if not os.path.exists(filename):
            raise ValueError('File does not exist.')

        # create new Job
        job = Job()

        # fill basic stuff
        job.submitted = datetime.datetime.now()

        # parse header
        header = Job.parse_pbs_header(filename)

        # we need at least a job name and number of cpus
        if 'name' not in header:
            raise ValueError('No job name given in PBS header.')
        if 'ncpus' not in header:
            raise ValueError('No ncpus given in PBS header.')

        # fill rest
        job.name = header['name']
        job.ncpus = header['ncpus']
        if 'nodes' in header:
            job.nodes = header['nodes']
        job.priority = header['priority'] if 'priority' in header else 0

        # return new job
        return job


__all__ = ['job']
