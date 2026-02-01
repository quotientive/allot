# Allot

*work in progress*

Allot is a lightweight task distributor written in Python designed to run Bash commands across multiple nodes via SSH. Inspiration from supercomputing environments, such as Aalto Triton and LUMI.


## Features

- SSH-based execution removes the need for remote agent installation.
- Progress can be read from the output of the run program.
- Automatically checks node status before issuing command.
- The commands issued can be customized with a function that enables the specification of `node_id`, `proc_id`, and `local_id` for the program.
- The state of the cluster can be saved and later restarted to check the progress (good progress printing not implemented).


## Prerequisites

- Python 3.10+ (no external dependencies)
- Shared Filesystem (for now)
- SSH Key-Based Authentication (no password input possible at this time)


## Example

Running a cluster (`example/example.py`):
```python
import time
import sys
from pathlib import Path
sys.path.append('../final')
from allot import Cluster, Task, NodeTaskConfig


def task_factory(ntc: NodeTaskConfig) -> Task:
    cmds = [
        "cd allot/example",
        f"python3 test.py {ntc.node_name} {ntc.proc_id}"
    ]
    output_file = ntc.output_dir / f"{ntc.node_name}-{ntc.proc_id}.out"
    task = Task(cmds, output_file=output_file, config=ntc)
    return task

clust = Cluster(
    job_name        = "example_job",
    output_dir      = Path.cwd() / "example" / "out",
    task_factory    = task_factory,
    nnodes          = 5,
    ntasks_per_node = 2,
    node_file       = "example/hostfile.txt"
)

while True:
    clust.print_progress()
    time.sleep(5)
```

Example program (`example/test.py`):
```python
import sys
import time

N = 120
def print_progress(i: int):
    print(f"\x02{i}/{N}\x03", flush=True)

print(f"arguments: {sys.argv}")
print_progress(0)

for i in range(N):
    print(f"this is iteration {i}")
    print_progress(i+1)
    time.sleep(1)

print_progress(N)
print("finished")
```


## Progress Monitoring

Allot looks for a specific pattern in `stdout` to track progress. By printing in the following format, progress can be tracked.

```
\x02[current_step]/[total_steps]\x03
```

It is advised to print `0/[total_steps]` at the beginning and `[total_steps]/[total_steps]` at the end to ensure `allot` knows the status.


## Configuration

The cluster can be configured similarly to Slurm job scripts with `ntasks` specifying the number of tasks, `nnodes` the number of nodes to use, and `ntasks_per_node` the number of tasks per individual node. At this moment, oversubscribing or a queue is not implemented.
