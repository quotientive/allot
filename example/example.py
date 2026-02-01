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
