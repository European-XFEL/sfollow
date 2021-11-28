"""Follow the output of a Slurm batch job"""

import codecs
import os
import re
import sys
from collections import defaultdict
from subprocess import run, PIPE

import trio

__version__ = '0.1'


STATES_FINISHED = {  # https://slurm.schedmd.com/squeue.html#lbAG
    'BOOT_FAIL',  'CANCELLED', 'COMPLETED',  'DEADLINE', 'FAILED',
    'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'SPECIAL_EXIT', 'TIMEOUT',
}
STATES_NOT_STARTED = {'PENDING', 'CONFIGURING'}

def job_states(job_ids):
    res = run([
        'squeue', '--noheader', '--format=%i %T', '--jobs', ','.join(job_ids)
    ], stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)
    return dict([l.strip().partition(' ')[::2] for l in res.stdout.splitlines()])

def fmt_jobs(job_ids):
    if len(job_ids) == 1:
        return f"Job {job_ids[0]}"
    elif len(job_ids) <= 3:
        return f"Jobs {','.join(job_ids)}"
    else:
        return f"{len(job_ids)} jobs"

def fmt_state(state):
    red, green, reset = '\x1b[31m', '\x1b[32m', '\x1b[0m'
    if state == 'COMPLETED':
        return f'{green}{state}{reset}'
    else:  # For now, treat finishing any other way as undesirable
        return f'{red}{state}{reset}'


async def watch_jobs(job_ids, nursery):
    states = job_states(job_ids)
    tail_cancels = defaultdict(list)

    for job_id, state in states.items():
        if state not in STATES_NOT_STARTED:
            info = get_job_info(job_id)
            for path in get_std_streams(info):
                nursery.start_soon(tail_log, path, job_id)

    spinner = '|/-\\'
    spinner_i = 0

    while True:
        job_ids_unfinished = [
            j for (j, s) in states.items() if s not in STATES_FINISHED
        ]
        if not job_ids_unfinished:
            break

        if all(st in STATES_NOT_STARTED for st in states.values()):
            print(
                f"[sfollow] {spinner[spinner_i]} Waiting for {fmt_jobs(job_ids)} "
                "to start", end='\r'
            )
            spinner_i = (spinner_i + 1) % len(spinner)

        for job_id, new_state in job_states(job_ids_unfinished).items():
            started = new_state not in STATES_NOT_STARTED
            if started and states[job_id] in STATES_NOT_STARTED:
                # Job started since the last check
                info = get_job_info(job_id)
                print(f"[sfollow] Job {job_id} ({info.get('JobName', '')}) started")
                for i, path in enumerate(get_std_streams(info)):
                    cscope = nursery.start(tail_log, path, True)
                    tail_cancels[job_id].append(cscope)

            finished = new_state in STATES_FINISHED
            if finished and states[job_id] not in STATES_FINISHED:
                # Job finished since the last check
                for cscope in tail_cancels.pop(job_id):
                    cscope.cancel()

                # Checkpoint - let tail tasks process any final output
                await trio.sleep(0)
                print(f'[sfollow] Job {job_id} finished ({fmt_state(new_state)})')

            states[job_id] = new_state

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
                    print(line)
                await trio.sleep(0.5)

        # The Slurm job has finished - this should catch any final output
        # Not unreachable, see https://youtrack.jetbrains.com/issue/PY-34484
        # noinspection PyUnreachableCode
        for line in sr:
            print(line)


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
        print("Stdout & stderr in the same file")
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
        print(f"Following your most recent job: {job_id} ({job_name})")
    trio.run(sfollow, job_ids)

if __name__ == '__main__':
    main()
