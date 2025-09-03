"""
Add an ensemble of gray-scott runs
"""

import sys, json, os


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
    for i in range(2):
        f = round(0.01 + i*0.01,3)
        for j in range(2):
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
