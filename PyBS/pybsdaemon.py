import asyncio
import logging
from contextlib import suppress
from sqlalchemy import func, or_
import os
import socket
import datetime
import subprocess

from .rpcserver import RpcServer
from .db import Database, Job


log = logging.getLogger(__name__)


class PyBSdaemon:
    def __init__(self, database, ncores=4, root_dir='/', mailer=None):
        self._rpc_server = RpcServer(self, 8888)
        self._task = None
        self._ncores = ncores
        self._root_dir = root_dir
        self._db = database
        self._mailer = mailer
        self._hostname = socket.gethostname()
        self._processes = {}

    async def open(self):
        await self._rpc_server.open()
        self._task = asyncio.ensure_future(self._process_jobs())

    async def close(self):
        self._rpc_server.close()
        await self._rpc_server.wait_closed()

    def _get_used_cpus(self, session):
        # get sum of Ncpu for running jobs
        ncpus = session \
            .query(func.sum(Job.ncores)) \
            .filter(Job.started != None) \
            .filter(Job.finished == None) \
            .first()

        # nothing?
        return 0 if ncpus[0] is None else int(ncpus[0])

    async def _process_jobs(self):
        while True:
            # open session
            with self._db() as session:
                # start as many jobs as possible
                while True:
                    # number of available cores
                    available_cores = self._ncores - self._get_used_cpus(session)

                    # start job if possible
                    if not await self._start_job(session, available_cores):
                        break

            # sleep a little
            await asyncio.sleep(1)

    async def _start_job(self, session, available_cores):
        # find job to process and lock row
        query = session.query(Job)

        # not started, not finished, not too many requested cores
        query = query.filter(Job.started == None, Job.finished == None, Job.ncores <= available_cores) \

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

        # and finally start job
        asyncio.ensure_future(self.run_job(job.id))

        # successfully started a job
        return True

    async def run_job(self, job_id):
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

                # send email?
                if 'send_mail' in header and 'email' in header and self._mailer is not None:
                    # really send?
                    self._mailer.send(header, job, return_code, outs, errs)

        # log it
        log.info('Finished job %d from %s...', job_id, filename)

    async def stop(self):
        self._db_pool.close()
        await self._db_pool.wait_closed()

        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    def list(self, started=True, finished=False, sort_asc=False, limit=None):
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
                    'ncores': job.ncores,
                    'priority': job.priority,
                    'nodes': job.nodes,
                    'filename': os.path.join(self._root_dir, job.filename),
                    'started': None if job.started is None else job.started.timestamp(),
                    'finished': None if job.finished is None else job.finished.timestamp()
                })
            return data

    def submit(self, filename, user):
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

    def remove(self, job_id: int):
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
