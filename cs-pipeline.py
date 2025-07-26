from clearml import Task
from clearml.automation import PipelineController

def pre_execute_callback_example(a_pipeline, a_node, current_param_override):
    # type (PipelineController, PipelineController.Node, dict) -> bool
    print(
        "Cloning Task id={} with parameters: {}".format(
            a_node.base_task_id, current_param_override
        )
    )
    # if we want to skip this node (and subtree of this node) we return False
    # return True to continue DAG execution
    return True


def post_execute_callback_example(a_pipeline, a_node):
    # type (PipelineController, PipelineController.Node) -> None
    print("Completed Task id={}".format(a_node.executed))
    # if we need the actual executed Task: Task.get_task(task_id=a_node.executed)
    return


default_queue = "q-team-a-cpu-050"
base_task_project = "cnasg-tk/CustomerSupport"
base_task_01 = "01-download-customer-support-dataset"
base_task_02 = "02-exploratory-data-analysis"
base_task_03 = "03-fine-tune-llama3-2-1b"
base_task_04 = "04-merge-lora-adapter-with-base-model"
base_task_05 = "05-deploy-merged-model"

# Connecting ClearML with the current pipeline,
# from here on everything is logged automatically
pipe = PipelineController(
    project="cnasg-tk/CustomerSupport", name="customer-service-pipeline", add_pipeline_tags=False
)

pipe.set_default_execution_queue(default_queue)

pipe.add_step(
    name = base_task_01,
    base_task_project = base_task_project,
    base_task_name = base_task_01,
    pre_execute_callback = pre_execute_callback_example,
    post_execute_callback = post_execute_callback_example,
)

pipe.add_step(
    parents = [base_task_01],
    name = base_task_02,
    base_task_project = base_task_project,
    base_task_name = base_task_02,
    pre_execute_callback = pre_execute_callback_example,
    post_execute_callback = post_execute_callback_example,
)

pipe.add_step(
    parents = [base_task_02],
    name = base_task_03,
    base_task_project = base_task_project,
    base_task_name = base_task_03,
    pre_execute_callback = pre_execute_callback_example,
    post_execute_callback = post_execute_callback_example,
)

pipe.add_step(
    parents = [base_task_03],
    name = base_task_04,
    base_task_project = base_task_project,
    base_task_name = base_task_04,
    pre_execute_callback = pre_execute_callback_example,
    post_execute_callback = post_execute_callback_example,
    parameter_override={
        "General/previous_task_id": "${03-fine-tune-llama3-2-1b.id}",
    }
)

pipe.add_step(
    parents = [base_task_04],
    name = base_task_05,
    base_task_project = base_task_project,
    base_task_name = base_task_05,
    pre_execute_callback = pre_execute_callback_example,
    post_execute_callback = post_execute_callback_example,
)

# for debugging purposes use local jobs
# pipe.start_locally()

# Starting the pipeline (in the background)
pipe.start(queue = default_queue)

print("done")