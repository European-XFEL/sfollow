import argparse
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

def job_states(job_ids, cluster=None):
    cmd = ['squeue', '--noheader', '--format=%i %T', '--jobs', ','.join(job_ids), '--states=all']
    if cluster is not None:
        cmd += ['--clusters', cluster]
    res = run(cmd, stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)
    return dict([l.strip().partition(' ')[::2] for l in res.stdout.splitlines()])


def sfollow(job_ids, cluster=None):
    states = job_states(job_ids, cluster=cluster)
    open_files = defaultdict(list)

    def finished(jid, final_state):
        # Job finished since the last check
        for fh in open_files.pop(jid, ()):
            # strip any trailing newline, let print() add one.
            b = fh.read()
            if b:
                print(b.decode('utf-8', 'replace').rstrip('\n'))
            fh.close()

        msg(f'Job {jid} finished ({fmt_state(final_state)})')

    # Jobs already running before we started: jump to near the end, like tail -f
    for job_id, state in states.items():
        if state not in STATES_NOT_STARTED:
            info = get_job_info(job_id, cluster=cluster)
            for path in get_std_streams(info):
                fh = open(path, 'rb')
                if os.stat(fh.fileno()).st_size > 512:
                    fh.seek(-512, os.SEEK_END)
                open_files[job_id].append(fh)

            if state in STATES_FINISHED:
                finished(job_id, state)

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
            ], cluster=cluster)
        else:
            new_states = {}

        for job_id, new_state in new_states.items():
            started = new_state not in STATES_NOT_STARTED
            if started and states[job_id] in STATES_NOT_STARTED:
                # Job started since the last check
                info = get_job_info(job_id, cluster=cluster)
                clear_spinner()
                msg(f"Job {job_id} ({info.get('JobName', '')}) started")
                for path in get_std_streams(info):
                    open_files[job_id].append(open(path, 'rb'))

            if new_state in STATES_FINISHED:
                finished(job_id, new_state)

            states[job_id] = new_state

        if all(st in STATES_FINISHED for st in states.values()):
            break  # All jobs finished, sfollow can exit

        time.sleep(0.5)


def get_job_info(job_id, cluster=None):
    """Return a dict of job info from 'scontrol show job'"""
    cmd = ['scontrol', 'show', 'job', str(job_id)]
    if cluster is not None:
        cmd += ['--clusters', cluster]
    out = run(cmd, stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)

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


def my_last_job(cluster=None):
    # '--format=%i %j' gives job IDs & names
    # --sort=-V sorts by submission time (descending)
    cmd = ['squeue', '--me', '--noheader', '--format=%i %j', '--sort=-V', '--states=all']
    if cluster is not None:
        cmd += ['--clusters', cluster]
    res = run(
        cmd, stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True
    )
    my_jobs = res.stdout.splitlines()
    if not my_jobs:
        raise UsageError("You have no jobs running or recently finished")
    return my_jobs[0].strip().split(maxsplit=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        '-M', '--clusters', metavar='CLUSTER',
        help="Name of the cluster running jobs, if not the default cluster"
    )
    ap.add_argument(
        "job_id", nargs='*',
        help="Number of job(s) to follow. If omitted, finds your most recent job."
    )
    args = ap.parse_args()
    job_ids = args.job_id

    try:
        if not job_ids:
            job_id, job_name = my_last_job(cluster=args.clusters)
            job_ids = [job_id]
            msg(f"Following your most recent job: {job_id} ({job_name})")

        sfollow(job_ids, cluster=args.clusters)
    except (KeyboardInterrupt, UsageError) as e:
        clear_spinner()
        print(e)
        return 1

if __name__ == '__main__':
    sys.exit(main())
