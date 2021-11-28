import sys
from shutil import get_terminal_size

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

def msg(s):
    print('[sfollow]', s, file=sys.stderr)

spinner_i = 0
spinner_parts = '|/-\\'
spinner_shown = False

def spinner_msg(s):
    global spinner_i, spinner_shown
    c = spinner_parts[spinner_i]
    print('[sfollow]', c, s, end='\r', file=sys.stderr)

    spinner_shown = True
    spinner_i = (spinner_i + 1) % len(spinner_parts)

def clear_spinner():
    global spinner_shown
    if spinner_shown:
        cols, _ = get_terminal_size()
        print(' ' * cols, end='\r')
        spinner_shown = False
