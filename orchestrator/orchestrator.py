import os, multiprocessing, subprocess, json, sys, shutil


def get_node_count():
    return os.environ.get("SLURM_JOB_NUM_NODES", 1)


def has_slurm():
    return any(var.startswith("slurm") for var in os.environ.keys())


def form_mpi_launch_cmd(cpu_count):
    if has_slurm():
        return f"srun -n {cpu_count} -N 1 ".split()
    else:
        return f"mpirun -np {cpu_count} ".split()


def create_dir(path):
    os.makedirs(path)


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
        create_dir(dirname)

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
            subprocess.run(run_cmd)
        except subprocess.CalledProcessError as e:
            print(f"Run failed with {e.returncode}")


def add_gs_runs_to_q(q):
    """
    Adds a list of gray-scott configurations to the queue
    Each object added to the queue is a dict of {"dirname", "json"}.
    :param q: Multiprocessing queue
    :return: None
    """
    with open(sys.argv[1]) as f:
        gs_json = json.load(f)

    for i in range(10):
        f = 0.01 + i*0.01
        for j in range(10):
            k = 0.05 + j*0.05

            gs_json["F"] = f
            gs_json["k"] = k
            dirname = f"F_{f}-k_{k}"
            q.put({"dirname": dirname, "json": gs_json})


def validate_gs(jsonfile):
    required = {"gs_exe", "gs_json", "adios2_xml"}
    with open(jsonfile) as f:
        input = json.load(f)
        assert required.issubset(input.keys()), f"Need {sys.argv[1]} to contain {required}"


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
