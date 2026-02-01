import subprocess
from dataclasses import dataclass, asdict
import os
from pathlib import Path as P
import re
import time
from enum import IntEnum, auto
import math
from typing import Callable
import shlex
import json
import logging
import sys

logger = logging.getLogger("allot")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s') # - %(asctime)s - 
handler.setFormatter(formatter)
logger.addHandler(handler)


PROGRESS_WIDTH = 50
GLOBAL_TIMEOUT = 10
FILE_MODIFICATION_TIMEOUT = 2 * 60 * 60


class NodeStatus(IntEnum):
    DOWN     = auto()
    CHECKING = auto()
    UP       = auto()

class TaskStatus(IntEnum):
    NOT_STARTED = auto()
    FINISHED    = auto()
    RUNNING_EARLY = auto()
    RUNNING     = auto()
    STALLED     = auto()

class ClusterStatus(IntEnum):
    UNINITIALISED = auto()
    RUNNING = auto()
    FINISHED = auto()


@dataclass
class NodeTaskConfig:
    cpus_on_node: int
    job_name:     str
    node_name:    str
    output_dir:   P
    local_id:     int  # [ 0   1] [ 0   1]
    node_id:      int  # [ 0   0] [ 1   1]
    proc_id:      int  # [ 0   1] [ 2   3]

    @classmethod
    def from_dict(cls, data: dict):
        data['output_dir'] = P(data['output_dir'])
        return cls(**data)


@dataclass
class Task:
    command:      list[str]
    output_file:  P
    config:       NodeTaskConfig # should not be here, but is just for convenience
    status:       TaskStatus = TaskStatus.NOT_STARTED
    last_update:  int = 0
    progress:     int = 0
    total:        int = 0
    # add file pointer to seek large logs

    @classmethod
    def from_dict(cls, data: dict):
        data['output_file'] = P(data['output_file'])
        data['config'] = NodeTaskConfig.from_dict(data['config'])
        data['status'] = TaskStatus(data['status'])
        return cls(**data)
        




class Node:

    TIMEOUT = GLOBAL_TIMEOUT

    def __init__(self, name: str, address: str, tasks: list[Task] | None = None) -> None:
        self.name = name
        self.address = address
        self.nproc = 0
        self.status = NodeStatus.CHECKING
        self.status_process: subprocess.Popen | None = None
        self.tasks: list[Task] = tasks if tasks is not None else []
        self.status_process = self.__send_command('nproc', stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.get_and_update_status()


    def to_dict(self) -> dict:

        def task_to_dict(task: Task):
            task_dict = asdict(task)
            print(task_dict)
            task_dict['status'] = task_dict['status'].value
            task_dict['output_file'] = str(task_dict['output_file'])
            task_dict['config']['output_dir'] = str(task_dict['config']['output_dir'])
            return task_dict
        return {
            'name': self.name,
            'address': self.address,
            'nproc': self.nproc,
            'status_process': None,
            'tasks': [task_to_dict(task) for task in self.tasks]
        }


    @classmethod
    def from_dict(cls, data: dict):
        tasks = [Task.from_dict(t) for t in data['tasks']]
        obj: Node = cls(data['name'], data['address'], tasks)
        return obj


    def assign_task(self, task: Task):
        cmd = shlex.quote('; '.join(task.command))
        try:
            task.output_file.unlink()
        except Exception:
            pass
        final_cmd = f"nohup bash -c {cmd} > {task.output_file} 2>&1 &"
        task.status = TaskStatus.RUNNING_EARLY
        self.__send_command(final_cmd, env=os.environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.tasks.append(task)


    def __send_command(self, command: str, **popen_kwargs) -> subprocess.Popen:
        logger.info(f"{self.name} ({self.address}): sending command {command}")
        process = subprocess.Popen(['ssh', '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={self.TIMEOUT}', self.address, command], **popen_kwargs)
        return process


    def get_and_update_status(self) -> NodeStatus:

        match self.status:

            case NodeStatus.DOWN:
                return NodeStatus.DOWN

            case NodeStatus.CHECKING:
                assert self.status_process is not None, "Status process should be defined"
                res = self.status_process.poll()
                if res is None:
                    return NodeStatus.CHECKING
                if self.status_process.returncode != 0:
                    self.status = NodeStatus.DOWN
                else:
                    stdout, _ = self.status_process.communicate()
                    self.nproc = int(stdout)
                    self.status = NodeStatus.UP
                return self.status

            case NodeStatus.UP:
                for task in self.tasks:
                    self.__update_task(task)
                return NodeStatus.UP

            case _:
                raise Exception("unreachable")


    def __update_task(self, task: Task):

        match task.status:
            case TaskStatus.NOT_STARTED:
                print("The tasks should have started already...")
                return task.status

            case TaskStatus.RUNNING_EARLY:
                if task.output_file.exists():
                    task.status = TaskStatus.RUNNING
                    return self.__update_task(task) # should recurse only once
                return TaskStatus.RUNNING_EARLY

            case TaskStatus.STALLED | TaskStatus.FINISHED:
                # no further actions done
                return task.status

            case TaskStatus.RUNNING:
                modification_time = int(task.output_file.stat().st_mtime)
                if modification_time + FILE_MODIFICATION_TIMEOUT <= time.time():
                    task.status = TaskStatus.STALLED
                    return task.status
                elif modification_time == task.last_update:
                    return task.status

                with open(task.output_file, 'r') as f:
                    filestr = f.read()
                progress_instances = re.findall(r"\x02(\d+)/(\d+)\x03", filestr)
                if len(progress_instances) == 0:
                    # optimally the user should print 0/N at the very beginning
                    return TaskStatus.RUNNING
                
                progress, total = progress_instances[-1] # consider last progress
                task.progress, task.total = int(progress), int(total)
                if task.progress == task.total:
                    task.status = TaskStatus.FINISHED
                return task.status

            case _:
                raise Exception("unreachable")



class Cluster:

    NODE_TASK_DENSITY = 4

    def __init__(
        self,
        job_name: str,
        output_dir: str | P,
        task_factory: Callable[[NodeTaskConfig],Task],
        ntasks = 0,
        nnodes = 0,
        ntasks_per_node = 0,
        nodes: list[Node] | None = None,
        node_list: list[tuple[str,str]] | None = None, # rename 
        node_file: str | P | None = None,
        restore_nodes: list[Node] | None = None,
    ) -> None:
        
        logger.info(f"initialising cluster {job_name=}, {output_dir=}")
        self.job_name = job_name
        self.nodes: list[Node] = []
        self.output_dir = P(output_dir).expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.__set_params(ntasks, nnodes, ntasks_per_node)

        if nodes is not None:
            pass # nodes = nodes
        elif node_list is not None:
            nodes = [Node(name, address) for name, address in node_list]
        elif node_file is not None:
            nodes = self.__read_nodes_from_file(P(node_file))
        elif restore_nodes is not None:
            self.nodes = restore_nodes
        else:
            raise Exception("Nodes must be defined through `nodes`, `node_list`, or `node_file`")

        if nodes is not None:
            self.__init_nodes(nodes)
        if task_factory is not None:
            self.__assign_tasks_to_nodes(task_factory)



    def to_dict(self) -> dict:
        data = {
            'job_name': self.job_name,
            'restore_nodes': [node.to_dict() for node in self.nodes],
            'output_dir': str(self.output_dir.resolve().absolute()),
            'task_factory': None,
            'ntasks': self.ntasks,
            'nnodes': self.nnodes,
            'ntasks_per_node': self.ntasks_per_node
        }
        return data

    @classmethod
    def from_dict(cls, data: dict):
        data['restore_nodes'] = [Node.from_dict(node) for node in data['restore_nodes']]
        return cls(**data)
    
    def write_to_json(self, filepath: P | str):
        with open(filepath, 'w') as f:
            f.write(json.dumps(self.to_dict()))

    @classmethod
    def read_from_json(cls, filepath: P | str):
        with open(filepath, 'r') as f:
            json_data = json.load(f)
        return cls.from_dict(json_data)


    def __assign_tasks_to_nodes(self, func: Callable[[NodeTaskConfig],Task]):

        local_id, node_id, proc_id = 0, 0, 0
        for node_id in range(self.nnodes):
            selected_node: Node = self.nodes[node_id]
            for local_id in range(self.ntasks_per_node):
                proc_id = node_id * self.ntasks_per_node + local_id
                conf = NodeTaskConfig(
                    cpus_on_node=selected_node.nproc, node_name=selected_node.address,
                    local_id=local_id, node_id=node_id, proc_id=proc_id, output_dir=self.output_dir,
                    job_name=self.job_name)
                task = func(conf)
                selected_node.assign_task(task)
    

    def update_node_statuses(self):
        for node in self.nodes:
            node.get_and_update_status()
            for task in node.tasks:
                if task.status in [TaskStatus.FINISHED, TaskStatus.STALLED]:
                    continue


    def __set_params(self, ntasks = 0, nnodes = 0, ntasks_per_node = 0):

        logger.info(f"setting parameters: {ntasks=}, {nnodes=}, {ntasks_per_node=}")
        has_ntasks = ntasks > 0
        has_nodes = nnodes > 0
        has_ntasks_per_node = ntasks_per_node > 0

        match (has_ntasks, has_nodes, has_ntasks_per_node):
            case (True, False, False):
                self.ntasks = ntasks
                self.nnodes = int(math.ceil(ntasks / self.NODE_TASK_DENSITY))
                self.ntasks_per_node = self.NODE_TASK_DENSITY
            case (False, True, False):
                self.ntasks = nnodes
                self.nnodes = nnodes
                self.ntasks_per_node = 1
            case (True, True, False):
                self.ntasks = ntasks
                self.nnodes = nnodes
                self.ntasks_per_node = int(math.ceil(ntasks / nnodes))
            case (False, True, True):
                self.ntasks = nnodes * ntasks_per_node
                self.nnodes = nnodes
                self.ntasks_per_node = ntasks_per_node
            case (True, False, True):
                self.ntasks = ntasks
                self.nnodes = int(math.ceil(ntasks / ntasks_per_node))
                self.ntasks_per_node = ntasks_per_node
            case (True, True, True):
                if ntasks > (nnodes * ntasks_per_node):
                    raise Exception(f"Invalid parameter configuration: {ntasks=}, {nnodes=}, {ntasks_per_node=}")
                self.ntasks = ntasks
                self.nnodes = nnodes
                self.ntasks_per_node = ntasks_per_node
            case _:
                raise Exception(f"Invalid parameter configuration: {ntasks=}, {nnodes=}, {ntasks_per_node=}")


    def __read_nodes_from_file(self, filepath: P) -> list[Node]:

        logger.info(f"reading nodes from file {filepath}")
        with open(filepath, 'r') as f:
            lines = f.readlines()
        nodes = []
        for line in lines:
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            split_line = line.split(':')
            if len(split_line) != 2:
                logger.info(f"    not read: {line}")
                continue
            name, address = split_line
            name, address = name.strip(), address.strip()
            logger.info(f"        read: {name=}, {address=}")
            nodes.append(Node(name, address))
        return nodes


    def __init_nodes(self, nodes: list[Node]) -> int:

        logger.info("initialising nodes") 
        nodes_to_check = nodes.copy()
        while len(nodes_to_check) > 0 and len(self.nodes) != self.nnodes:
            node = nodes_to_check.pop(0)
            node_status = node.get_and_update_status()
            match node_status:
                case NodeStatus.DOWN:
                    logger.warning(f"node ({node.name}, {node.address}) is down")
                    continue
                case NodeStatus.CHECKING:
                    nodes_to_check.append(node)
                case NodeStatus.UP:
                    logger.warning(f"node ({node.name}, {node.address}) is up")
                    self.nodes.append(node)

        if len(self.nodes) < self.nnodes:
            raise Exception(f"too few nodes available for the number of requested nodes ({len(self.nodes)} < {self.nnodes})")

        return len(self.nodes) 


    def print_progress(self):
        lines_printed = 0

        self.update_node_statuses()
        for node_id, node in enumerate(self.nodes):
            print(f"Node: {node.name} | {node.status} | {node_id=}")
            lines_printed += 1
            for task in node.tasks:
                print(f"    Task: {task.status}, local_id={task.config.local_id}, proc_id={task.config.proc_id}, progress: {task.progress}/{task.total}")
                lines_printed += 1
            lines_printed = 0 # can be replaced with multiplication
        print("")
