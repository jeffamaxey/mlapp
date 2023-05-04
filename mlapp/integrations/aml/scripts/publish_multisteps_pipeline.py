from azureml.data.data_reference import DataReference
from azureml.pipeline.core import PipelineData

from mlapp.integrations.aml.utils.compute import get_or_create_compute_target
from mlapp.integrations.aml.utils.constants import OUTPUT_PATH_ON_COMPUTE, DATA_REFERENCE_NAME
from mlapp.integrations.aml.utils.pipeline import publish_pipeline_endpoint, create_mlapp_pipeline_step
from mlapp.integrations.aml.utils.runconfig import create_runconfig
import os


def run_script(ws, datastore, pipeline_name, instructions):
    pipeline_steps = []
    last_output = []

    for i in range(len(instructions)):
        compute_target = get_or_create_compute_target(
            ws,
            compute_name=instructions[i]['name'],
            vm_size=instructions[i].get('vm_size', 'STANDARD_D2_V2'),
            min_nodes=instructions[i].get('min_nodes', 0),
            max_nodes=instructions[i].get('max_nodes', 4),
            idle_sec=instructions[i].get('idle_seconds_before_scale_down', 120)
            )
        run_config = create_runconfig(compute_target)

        # input directory in datastore
        input_dir = last_output if last_output else None
        # output directory in datastore
        output_dir = PipelineData(
            name=DATA_REFERENCE_NAME + str(i),
            datastore=datastore,
            output_path_on_compute=OUTPUT_PATH_ON_COMPUTE
        )

        # create pipeline step
        pipeline_step = create_mlapp_pipeline_step(
            compute_target,
            run_config,
            source_directory=os.getcwd(),
            entry_script=os.path.join("deployment", "aml_flow.py"),
            input_dir=input_dir,
            output_dir=output_dir,
            param_name=f'config{str(i)}',
        )

        # add to pipeline
        pipeline_steps += pipeline_step

        # reference last output
        last_output.append(output_dir)

    publish_pipeline_endpoint(ws, pipeline_steps, pipeline_name)


