import asyncio
import datetime
import logging
import os
import socket
import subprocess

from sqlalchemy import or_
from sqlalchemy.orm import Query

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

    async def _main_loop(self):
        """Main loop for daemon that starts new jobs."""

        # sleep a little, before we start jobs
        await asyncio.sleep(10)

        # Run forever
        while True:
            # catch exceptions
            try:
                # open session
                with self._db() as session:
                    # start as many jobs as possible
                    while True:
                        # sleep a little
                        await asyncio.sleep(1)

                        # number of available CPUs
                        available_cpus = self._ncpus - self._used_cpus

                        # start job if possible
                        if not await self._start_job(session, available_cpus):
                            break
            except:
                log.exception('Something went wrong.')

            # sleep a little
            await asyncio.sleep(10)

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
        query = query.filter(or_(Job.nodes == None, Job.nodes == self._hostname, Job.nodes.like(self._hostname + ',%'),
                                 Job.nodes.like('%,' + self._hostname + ',%'), Job.nodes.like('%,' + self._hostname)))

        # sort by priority and by oldest first
        query = query.order_by(Job.priority.desc(), Job.submitted.asc())

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
        job_id = job.id
        log.info('Preparing job %d...', job_id)
        asyncio.ensure_future(self._run_job(job_id))

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

            # log it
            log.info('Starting job %d from %s...', job_id, filename)

            # parse PBS header
            header = Job.parse_pbs_header(filename)

            # get working directory
            cwd = os.path.dirname(filename)

            # run job
            proc = await asyncio.create_subprocess_shell(filename, cwd=cwd,
                                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # store it
            self._processes[job_id] = proc

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

    def list_waiting(self):
        """Get a list of waiting jobs.

        Returns:
            List of dictionaries with job infos.
        """

        # get session
        with self._db() as session:
            # do query
            jobs = session \
                .query(Job) \
                .filter(Job.started == None, Job.finished == None) \
                .order_by(Job.priority.desc(), Job.submitted.asc())

            # return list
            return self._list(jobs)

    def list_running(self):
        """Get a list of running jobs.

        Returns:
            List of dictionaries with job infos.
        """

        # get session
        with self._db() as session:
            # do query
            jobs = session \
                .query(Job) \
                .filter(Job.started != None, Job.finished == None) \
                .order_by(Job.started.asc())

            # return list
            return self._list(jobs)

    def list_finished(self, limit: int = 5):
        """Get a list of running jobs.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of dictionaries with job infos.
        """

        # get session
        with self._db() as session:
            # do query
            jobs = session \
                .query(Job) \
                .filter(Job.started != None, Job.finished != None) \
                .order_by(Job.finished.desc())\
                .limit(limit)

            # return list
            return self._list(jobs)

    def _list(self, jobs: Query) -> list:
        """Get a list of jobs.

        Args:
            jobs: Query to execute.

        Returns:
            List of dictionaries with job infos.
        """
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
                raise ValueError('Job not found.')
            ncpus = job.ncpus

            # delete it
            log.info('Deleting job %d...', job_id)
            session.delete(job)

        # got a running process?
        if job_id in self._processes:
            # kill job
            log.info('Killing running process for job %s...', job_id)
            self._processes[job_id].kill()

        # free CPUs
        self._used_cpus -= ncpus

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

    def get_cpus(self) -> (int, int):
        """Returns the currently occupied and the total number of CPUs on this host.

        Returns:
            Tuple of currently occupied and total number of CPUs.
        """
        return (self._used_cpus, self._ncpus)


__all__ = ['PyBSdaemon']
