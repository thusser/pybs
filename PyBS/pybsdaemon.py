import asyncio
import logging
from sqlalchemy import func, or_
import os
import socket
import datetime
import subprocess

from .db import Job


log = logging.getLogger(__name__)


class PyBSdaemon:
    """The PyBS daemon that runs all jobs"""

    def __init__(self, database: 'Database', ncpus: int = 4, root_dir: str = '/', mailer: 'Mailer' = None):
        """Creates a new PyBS daemon.

        Args:
            database: Database to use for storing jobs.
            ncpus: Number of available CPUs on node.
            root_dir: Root directory for all jobs.
            mailer: Mailer instance for sending emails.
        """
        self._task = None
        self._ncpus = ncpus
        self._root_dir = root_dir
        self._db = database
        self._mailer = mailer
        self._hostname = socket.gethostname()
        self._processes = {}
        self._used_cpus = 0

        # start periodic task
        self._task = asyncio.ensure_future(self._main_loop())

    def close(self):
        """Close daemon."""
        self._task.cancel()

    def _get_used_cpus(self, session: 'sqlalchemy.orm.session') -> int:
        """Get number of currently used CPUs.

        Args:
            session: SQLAlchemy session to use.

        Returns:
            Number of used CPUs.
        """

        # get sum of Ncpu for running jobs on this node
        ncpus = session \
            .query(func.sum(Job.ncpus)) \
            .filter(Job.started != None) \
            .filter(Job.finished == None) \
            .filter(Job.nodes == self._hostname) \
            .first()

        # nothing?
        return 0 if ncpus[0] is None else int(ncpus[0])

    async def _main_loop(self):
        """Main loop for daemon that starts new jobs."""

        # Run forever
        while True:
            # open session
            with self._db() as session:
                # start as many jobs as possible
                while True:
                    # number of available CPUs
                    #available_cpus = self._ncpus - self._get_used_cpus(session)
                    available_cpus = self._ncpus - self._used_cpus

                    # start job if possible
                    if not await self._start_job(session, available_cpus):
                        break

            # sleep a little
            await asyncio.sleep(1)

    async def _start_job(self, session: 'sqlalchemy.orm.session', available_cpus: int) -> bool:
        """Try to start a new job.

        Args:
            session: SQLAlchemy session to use.
            available_cpus: number of available CPUs.

        Returns:
            Whether a new job has been started.
        """

        # find job to process and lock row
        query = session.query(Job)

        # not started, not finished, not too many requested cores
        query = query.filter(Job.started == None, Job.finished == None, Job.ncpus <= available_cpus)

        # if nodes is not NULL, _hostname must be at beginning, between two commas, or at end of nodes
        # this looks simpler, but works on MySQL only:
        #   .filter(or_(Job.nodes == None, func.find_in_set(self._hostname, Job.nodes) > 0))
        query = query.filter(or_(Job.nodes == None, Job.nodes.like(self._hostname + ',%'),
                                 Job.nodes.like('%,' + self._hostname + '%,'), Job.nodes.like('%,' + self._hostname)))

        # sort by priority and by oldest first
        query = query.order_by(Job.priority, Job.submitted.asc())

        # lock row for later update and pick first
        job = query.with_for_update().first()

        # none available?
        if job is None:
            return False

        # set Started
        job.started = datetime.datetime.now()
        session.flush()

        # use CPUs
        self._used_cpus += job.ncpus

        # and finally start job
        asyncio.ensure_future(self._run_job(job.id))

        # successfully started a job
        return True

    async def _run_job(self, job_id: int):
        """Prepare a job, run it, and analyse output.

        Args:
            job_id: ID of job to run.
        """

        header = {}
        try:
            # get job
            with self._db() as session:
                # get job
                job = session.query(Job).filter(Job.id == job_id).first()
                if job is None:
                    # could not find job in DB
                    return

                # set hostname
                job.nodes = self._hostname

                # store filename
                filename = os.path.join(self._root_dir, job.filename)

                # parse PBS header
                header = Job.parse_pbs_header(filename)

                # get working directory
                cwd = os.path.dirname(filename)

                # run job
                proc = await asyncio.create_subprocess_shell(filename, cwd=cwd,
                                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                # store it
                self._processes[job_id] = proc

                # log it
                log.info('Starting job %d from %s...', job_id, filename)

            # wait for process
            outs, errs = await proc.communicate()
            return_code = proc.returncode

            # write output and error
            for kind, lines in [('output', outs), ('error', errs)]:
                try:
                    if kind in header:
                        # write lines
                        with open(os.path.join(cwd, header[kind]), 'wb') as f:
                            f.write(lines)

                        # set file permissions
                        os.chmod(os.path.join(cwd, header[kind]), 0o664)
                except (IOError, PermissionError, ValueError):
                    # Could not write file.
                    pass

        finally:
            # remove process
            if job_id in self._processes:
                del self._processes[job_id]

            # set Finished
            with self._db() as session:
                # get job
                job = session.query(Job).filter(Job.id == job_id).first()
                if job is None:
                    # could not find job in DB
                    return

                # set finished and PID
                job.finished = datetime.datetime.now()

                # free CPUs
                self._used_cpus -= job.ncpus

                # send email?
                if 'send_mail' in header and 'email' in header and self._mailer is not None:
                    # really send?
                    self._mailer.send(header, job, return_code, outs, errs)

        # log it
        log.info('Finished job %d from %s...', job_id, filename)

    def list(self, started: bool = True, finished: bool = False, sort_asc: bool = False, limit: int = None) -> list:
        """Get a list of jobs.

        Args:
            started: Return only jobs that have (True) or have not (False) started.
            finished: Return only jobs that have (True) or have not (False) finished.
            sort_asc: Sort ascending (True) or descending (False).
            limit: Limit number of returned jobs.

        Returns:
            List of dictionaries with job infos.
        """

        # get session
        with self._db() as session:
            # do query
            jobs = session.query(Job)

            # which ones?
            order_column = Job.submitted
            if started and not finished:    # i.e. running jobs
                jobs = jobs.filter(Job.started != None, Job.finished == None)
                order_column = Job.started
            elif not started:               # i.e. waiting jobs
                jobs = jobs.filter(Job.started == None)
            elif finished:
                jobs = jobs.filter(Job.finished != None)
                order_column = Job.finished

            # sort
            if sort_asc:
                jobs = jobs.order_by(order_column.asc())
            else:
                jobs = jobs.order_by(order_column.desc())

            # limit
            if limit is not None:
                jobs = jobs.limit(limit)

            # extract data
            data = []
            for job in jobs:
                data.append({
                    'id': job.id,
                    'name': job.name,
                    'username': job.username,
                    'ncpus': job.ncpus,
                    'priority': job.priority,
                    'nodes': job.nodes,
                    'filename': os.path.join(self._root_dir, job.filename),
                    'started': None if job.started is None else job.started.timestamp(),
                    'finished': None if job.finished is None else job.finished.timestamp()
                })
            return data

    def submit(self, filename: str, user: str) -> dict:
        """Submit a new script to the queue.

        Args:
            filename: Name of file to submit.
            user: Name of user that submitted job.

        Returns:
            Dictionary with new job ID.
        """

        # file exists?
        if not os.path.exists(filename):
            raise ValueError('File does not exist.')

        # get session
        with self._db() as session:
            # create job
            job = Job.from_file(filename)

            # set username and filename
            job.username = user
            job.filename = os.path.relpath(filename, self._root_dir)

            # add to database
            session.add(job)
            session.flush()
            jobid = job.id

            # log it
            log.info('Submitted new job %s with ID %d.', filename, jobid)

        # return ID of new job
        return {'id': jobid}

    def remove(self, job_id: int) -> dict:
        """Remove an existing job.

        Args:
            job_id: ID of job to remove.

        Returns:
            Dictionary with success message.
        """

        # get job
        with self._db() as session:
            # get job
            job = session.query(Job).filter(Job.id == job_id).first()
            if job is None:
                # could not find job in DB
                ValueError('Job not found.')

            # delete it
            log.info('Deleting job %d...', job_id)
            session.delete(job)

        # got a running process?
        if job_id in self._processes:
            # kill job
            log.info('Killing running process for job %s...', job_id)
            self._processes[job_id].kill()

        # send success
        return {'success': True}

    def run(self, job_id: int) -> dict:
        """Start a waiting job now.

        Args:
            job_id: ID of job to start.

        Returns:
            Dictionary with success message.
        """

        # get job
        with self._db() as session:
            # get job
            job = session.query(Job).filter(Job.id == job_id).first()
            if job is None:
                # could not find job in DB
                ValueError('Job not found.')

            # set started
            job.started = datetime.datetime.now()
            session.flush()

            # and finally start job
            asyncio.ensure_future(self._run_job(job.id))

        # send success
        return {'success': True}


__all__ = ['PyBSdaemon']
