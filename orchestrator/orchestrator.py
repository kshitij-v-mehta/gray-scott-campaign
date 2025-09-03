"""
This is a simple orchestrator that will run an ensemble of gray-scott simulations.
Each instance is run on one full node using all CPUs of the node.
No restart capabilities are provided yet to resume a partially completed job.
"""

import os, multiprocessing, subprocess, json, sys, shutil


def get_node_count():
    return int(os.environ.get("SLURM_JOB_NUM_NODES", 1))


def has_slurm():
    return any(var.startswith("SLURM") for var in os.environ.keys())


def form_mpi_launch_cmd(cpu_count):
    if has_slurm():
        return f"srun -n {cpu_count} -N 1".split()
    else:
        return f"mpirun -np {cpu_count} ".split()


def process_f(q):
    """
    Function executed by each process launched by the main orchestrator.
    The process will retrieve a task from the queue and launch a gray-scott run.
    Each run will utilize a single node and use all CPUs on it.
    :param q: Multiprocessing.Queue
    :return: None
    """
    while True:
        task = q.get()
        if task is None:
            break

        dirname = task['dirname']
        jsondata = task['json']

        # Create ensemble run directory
        os.makedirs(dirname)

        # Place json in run directory
        with open(os.path.join(dirname, "settings-files.json"), 'w') as f:
            json.dump(jsondata, f, indent=4)

        # Place adios2.xml in run directory
        with open(sys.argv[1]) as f:
            input = json.load(f)
            adios2_xml_src = input['adios2_xml']
            gs_exe = input['gs_exe']

        shutil.copy(adios2_xml_src, os.path.join(dirname, "adios2.xml"))

        # Form launch command
        cpu_count = multiprocessing.cpu_count()
        run_cmd = form_mpi_launch_cmd(cpu_count)
        run_cmd.extend(f"{gs_exe} settings-files.json".split())

        try:
            print(f"Process {os.getgid()} launching {dirname} as {run_cmd}")

            stdout = os.path.join(dirname, "stdout.txt")
            stderr = os.path.join(dirname, "stderr.txt")
            with open(stdout, 'w') as stdout_f, open(stderr, 'w') as stderr_f:
                subprocess.run(run_cmd, cwd=dirname, stdout=stdout_f, stderr=stderr_f, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Run failed with {e.returncode}")


def add_gs_runs_to_q(q):
    """
    Adds a list of gray-scott configurations to the queue
    Each object added to the queue is a dict of {"dirname", "json"}.
    :param q: Multiprocessing queue
    :return: None
    """
    # Get the path to the gray-scott json and the path to the root ensemble directory
    with open(sys.argv[1]) as f:
        input_settings = json.load(f)
        gs_json_src = input_settings['gs_json']
        ensemble_root = input_settings['ensemble_root']

    # Read the gray-scott json file
    with open(gs_json_src) as f:
        gs_json = json.load(f)

    # Iterate over F and k and add them to the queue
    run_count = 0
    for i in range(10):
        f = round(0.01 + i*0.01,3)
        for j in range(10):
            k = round(0.05 + j*0.05,3)

            gs_json["F"] = f
            gs_json["k"] = k
            dirname = os.path.join(ensemble_root, f"F_{f}-k_{k}")
            if os.path.exists(dirname):
                print(f"Skipping {dirname} as it already exists")
                continue
            q.put({"dirname": dirname, "json": gs_json})
            run_count += 1

    print(f"Orchestrator created {run_count} runs")


def validate_gs(jsonfile):
    """
    Validates that the input settings configuration file to the orchestrator is a json containing the keys
    gs_exe, gs_json, adios2_xml, and ensemble_root.
    :param jsonfile: Input configuration in json format to the orchestrator
    :return: None
    """
    required = {"gs_exe", "gs_json", "adios2_xml", "ensemble_root"}
    with open(jsonfile) as f:
        input = json.load(f)
        assert required.issubset(input.keys()), f"Need {sys.argv[1]} to contain {required}"

        assert all(os.path.exists(path) for path in input.values()), f"Input file {jsonfile} does not exist"


def main():
    assert len(sys.argv) == 2, "Provide a local settings json file that points to gray-scott exe and input files"
    validate_gs(sys.argv[1])

    q = multiprocessing.Queue()
    node_count = get_node_count()
    process_list = list()

    # Start a process for each node in this job
    for _ in range(node_count):
        p = multiprocessing.Process(target=process_f, args=(q,))
        p.start()
        process_list.append(p)
    print(f"Started {len(process_list)} processes")

    # Add gray-scott json configurations to queue
    add_gs_runs_to_q(q)

    # Send termination signal to all processes
    for _ in process_list:
        q.put(None)

    # Wait for all processes to finish
    for p in process_list:
        p.join()

    print("DONE")


if __name__ == "__main__":
    main()
