import itertools
import os
import re
import sys
import time
from collections import defaultdict
from subprocess import run, PIPE

from .terminal import msg, spinner_msg, clear_spinner, fmt_state, fmt_jobs


class UsageError(Exception):
    pass


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


def sfollow(job_ids):
    states = job_states(job_ids)
    open_files = defaultdict(list)

    # Jobs already running before we started: jump to near the end, like tail -f
    for job_id, state in states.items():
        if state not in STATES_NOT_STARTED:
            info = get_job_info(job_id)
            for path in get_std_streams(info):
                fh = open(path, 'rb')
                if os.stat(fh.fileno()).st_size > 512:
                    fh.seek(-512, os.SEEK_END)
                open_files[job_id].append(fh)

    for i in itertools.count():
        for files in open_files.values():
            for f in files:
                for line in f:
                    print(line.decode('utf-8', 'replace'), end='')

        # Check for jobs started/finished every 4th cycle (2 seconds)
        if i % 4 == 0:
            if not open_files:
                spinner_msg(f"Waiting for {fmt_jobs(job_ids)} to start")

            new_states = job_states([
                j for (j, s) in states.items() if s not in STATES_FINISHED
            ])
        else:
            new_states = {}

        for job_id, new_state in new_states.items():
            started = new_state not in STATES_NOT_STARTED
            if started and states[job_id] in STATES_NOT_STARTED:
                # Job started since the last check
                info = get_job_info(job_id)
                clear_spinner()
                msg(f"Job {job_id} ({info.get('JobName', '')}) started")
                for path in get_std_streams(info):
                    open_files[job_id].append(open(path, 'rb'))

            if new_state in STATES_FINISHED:
                # Job finished since the last check
                for fh in open_files.pop(job_id, ()):
                    # strip any trailing newline, let print() add one.
                    b = fh.read()
                    if b:
                        print(b.decode('utf-8', 'replace').rstrip('\n'))
                    fh.close()

                msg(f'Job {job_id} finished ({fmt_state(new_state)})')

            states[job_id] = new_state

        if all(st in STATES_FINISHED for st in states.values()):
            break  # All jobs finished, sfollow can exit

        time.sleep(0.5)


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


def my_last_job():
    # '--format=%i %j' gives job IDs & names
    # --sort=-V sorts by submission time (descending)
    res = run(
        ['squeue', '--me', '--noheader', '--format=%i %j', '--sort=-V', '--states=all'],
        stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True
    )
    my_jobs = res.stdout.splitlines()
    if not my_jobs:
        raise UsageError("You have no jobs running or recently finished")
    return my_jobs[0].strip().split(maxsplit=1)


def main():
    job_ids = sys.argv[1:]

    try:
        if not job_ids:
            job_id, job_name = my_last_job()
            job_ids = [job_id]
            msg(f"Following your most recent job: {job_id} ({job_name})")

        sfollow(job_ids)
    except (KeyboardInterrupt, UsageError) as e:
        clear_spinner()
        print(e)
        return 1

if __name__ == '__main__':
    sys.exit(main())
