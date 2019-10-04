import git

REPO = git.Repo(".", search_parent_directories=True)
EXAMPLES_ROOT = REPO.working_tree_dir + "/examples/k8s"
ALL_JOB_DEFINITION_NAMES = ["job-helloworld", "job-fail", "job-timeout", "job-template"]
