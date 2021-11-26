``sfollow`` is a tool to follow the stdout/stderr of a Slurm job submitted
with ``sbatch``.

This is similar to the ``sattach`` command, but that only works with job
*steps* started by ``srun``.

Installation::

    pip install sfollow

Usage

.. code-block:: shell

    # Specify a job ID
    sfollow 9251426

    # Follow your most recently submitted job
    sfollow
