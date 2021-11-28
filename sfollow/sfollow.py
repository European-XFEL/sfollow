import codecs
import os
import re
import sys
from collections import defaultdict
from subprocess import run, PIPE

import trio

from .terminal import msg, spinner_msg, clear_spinner, fmt_state, fmt_jobs


STATES_FINISHED = {  # https://slurm.schedmd.com/squeue.html#lbAG
    'BOOT_FAIL',  'CANCELLED', 'COMPLETED',  'DEADLINE', 'FAILED',
    'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'SPECIAL_EXIT', 'TIMEOUT',
}
STATES_NOT_STARTED = {'PENDING', 'CONFIGURING'}

def job_states(job_ids):
    res = run([
        'squeue', '--noheader', '--format=%i %T', '--jobs', ','.join(job_ids),
        '--states=all',
    ], stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)
    return dict([l.strip().partition(' ')[::2] for l in res.stdout.splitlines()])


async def watch_jobs(job_ids, nursery):
    states = job_states(job_ids)
    tail_cancels = defaultdict(list)

    for job_id, state in states.items():
        if state not in STATES_NOT_STARTED:
            info = get_job_info(job_id)
            for path in get_std_streams(info):
                cscope = await nursery.start(tail_log, path)
                tail_cancels[job_id].append(cscope)

    while True:
        job_ids_unfinished = [
            j for (j, s) in states.items() if s not in STATES_FINISHED
        ]

        if all(st in STATES_NOT_STARTED for st in states.values()):
            spinner_msg(f"Waiting for {fmt_jobs(job_ids)} to start")

        for job_id, new_state in job_states(job_ids_unfinished).items():
            started = new_state not in STATES_NOT_STARTED
            if started and states[job_id] in STATES_NOT_STARTED:
                # Job started since the last check
                info = get_job_info(job_id)
                clear_spinner()
                msg(f"Job {job_id} ({info.get('JobName', '')}) started")
                for i, path in enumerate(get_std_streams(info)):
                    cscope = await nursery.start(tail_log, path, True)
                    tail_cancels[job_id].append(cscope)

            if new_state in STATES_FINISHED:
                # Job finished since the last check
                for cscope in tail_cancels.pop(job_id):
                    cscope.cancel()

                # Fudge - hopefully final output will show before 'finished' msg
                await trio.sleep(0.01)
                msg(f'Job {job_id} finished ({fmt_state(new_state)})')

            states[job_id] = new_state

        if all(st in STATES_FINISHED for st in states.values()):
            break

        await trio.sleep(2)


async def tail_log(path, newly_started=False, *, task_status=trio.TASK_STATUS_IGNORED):
    with open(path, 'rb') as fh:
        if not newly_started and os.stat(fh.fileno()).st_size > 512:
            fh.seek(-512, os.SEEK_END)

        sr = codecs.getreader('utf-8')(fh, 'replace')

        with trio.CancelScope() as cscope:
            task_status.started(cscope)

            while True:
                for line in sr:
                    print(line, end='')
                await trio.sleep(0.5)

        # The Slurm job has finished - this should catch any final output
        # Not unreachable, see https://youtrack.jetbrains.com/issue/PY-34484
        # noinspection PyUnreachableCode
        for line in sr:
            print(line, end='')


def get_job_info(job_id):
    """Return a dict of job info from 'scontrol show job'"""
    out = run(['scontrol', 'show', 'job', str(job_id)],
              stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)

    kvlist = re.split(r'(?:^|\s+)([A-Za-z:]+)=', out.stdout.strip())[1:]
    return dict(zip(kvlist[::2], kvlist[1::2]))


def get_std_streams(job_info):
    """Get a list of paths for stdout & stderr

    If they are the same file, only keep one.
    """
    paths = []
    if 'StdOut' in job_info:
        paths.append(job_info['StdOut'])
    if 'StdErr' in job_info:
        paths.append(job_info['StdErr'])

    if len(paths) == 2 and os.path.samefile(paths[0],  paths[1]):
        # Stdout & stderr in the same file
        del paths[1]

    return paths


async def sfollow(job_ids):
    """Follow the output from a SLURM batch job"""
    async with trio.open_nursery() as nursery:
        nursery.start_soon(watch_jobs, job_ids, nursery)
    # The nursery waits for all child tasks to finish, which should happen
    # shortly after the Slurm jobs in question finish.


def my_last_job():
    # '--format=%i %j' gives job IDs & names
    # --sort=-V sorts by submission time (descending)
    res = run(['squeue', '--me', '--noheader', '--format=%i %j', '--sort=-V'],
              stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)
    my_jobs = res.stdout.splitlines()
    if not my_jobs:
        raise Exception("You have no jobs running")
    return my_jobs[0].strip().split(maxsplit=1)


def main():
    if len(sys.argv) >= 2:
        job_ids = sys.argv[1:]
    else:
        job_id, job_name = my_last_job()
        job_ids = [job_id]
        msg(f"Following your most recent job: {job_id} ({job_name})")
    trio.run(sfollow, job_ids)

if __name__ == '__main__':
    main()
