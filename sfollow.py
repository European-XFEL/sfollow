from contextlib import ExitStack
import os
import re
import select
from subprocess import run, PIPE
import sys

def get_job_info(job_id):
    out = run(['scontrol', 'show', 'job', str(job_id)],
              stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)

    kvlist = re.split(r'(?:^|\s+)([A-Za-z:]+)=', out.stdout.strip())[1:]
    return dict(zip(kvlist[::2], kvlist[1::2]))


def get_std_streams(job_info):
    paths = []
    if 'StdOut' in job_info:
        paths.append(job_info['StdOut'])
    if 'StdErr' in job_info:
        paths.append(job_info['StdErr'])

    if len(paths) == 2 and os.path.samefile(paths[0],  paths[1]):
        print("Stdout & stderr in the same file")
        del paths[1]

    return paths

def multi_tail(paths):
    if not paths:
        print("No files to follow")
        return

    fhs = []
    initial_lines = []
    with ExitStack() as stack:
        for path in paths:
            print("Following", path)
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            if os.stat(fd).st_size > 512:
                os.lseek(fd, -512, os.SEEK_END)
            fh = open(fd, encoding='utf-8', errors='replace')
            initial_lines.extend(fh.readlines()[-5:])
            fhs.append(stack.enter_context(fh))
        print()

        for line in initial_lines:
            print(line, end='')

        try:
            multi_tail_fhs(fhs)
        except KeyboardInterrupt:
            pass
    print()

def multi_tail_fhs(fhs):
   poller = select.poll()
   for fh in fhs:
       poller.register(fh, select.POLLIN | select.POLLPRI)

   fd_to_fh = {fh.fileno(): fh for fh in fhs}

   while True:
       for fd, _ in poller.poll():
           fh = fd_to_fh[fd]
           # Read all available data from this file handle
           while True:
                try:
                    chunk = fh.read(1024)
                except BlockingIOError:
                    break
                if not chunk: break
                print(chunk, end='')


def sfollow(job_id):
    job_info = get_job_info(job_id)
    paths = get_std_streams(job_info)
    multi_tail(paths)


def main():
    sfollow(sys.argv[1])

if __name__ == '__main__':
    main()
