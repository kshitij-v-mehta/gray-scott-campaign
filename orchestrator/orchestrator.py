import os, multiprocessing, subprocess


def get_node_count():
    return os.environ.get("SLURM_JOB_NUM_NODES", 1)


def process_f(q):
    while True:
        task = q.get()
        if task == 'quit':
            break

        cpu_count = multiprocessing.cpu_count()
        run_cmd = f"mpirun -np {cpu_count} echo {task}".split()

        try:
            p = subprocess.run(run_cmd)
        except subprocess.CalledProcessError as e:
            print(f"Run failed with {e.returncode}")


def main():
    q = multiprocessing.Queue()
    node_count = get_node_count()
    process_list = list()

    for _ in range(node_count):
        p = multiprocessing.Process(target=process_f, args=(q,))
        p.start()
        process_list.append(p)
    print(f"Started {len(process_list)} processes")

    for i in range(10):
        q.put(i)

    # Send termination signal
    for _ in process_list:
        q.put("quit")

    # Wait for all processes to finish
    for p in process_list:
        p.join()

    print("DONE")


if __name__ == "__main__":
    main()
