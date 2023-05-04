from azureml.core import Workspace
from azureml.core.authentication import InteractiveLoginAuthentication, AzureCliAuthentication, \
    ServicePrincipalAuthentication, MsiAuthentication


def init_workspace(tenant_id, subscription_id, resource_group, workspace_name):
    if tenant_id is None:
        return Workspace(
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_name=workspace_name,
        )
    interactive_auth = InteractiveLoginAuthentication(tenant_id=tenant_id)
    return Workspace(
        subscription_id=subscription_id,
        resource_group=resource_group,
        workspace_name=workspace_name,
        auth=interactive_auth,
    )
