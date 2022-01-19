import sys
import pytest

sys.path.append("src/")
from airbyte import AirbyteJobStatus, AirbyteConnectionSource, AirbyteSource

def test_airbyte_instance(airbyte_instance):

    assert airbyte_instance.airbyte_connection_id == "1234"
    assert airbyte_instance.airbyte_retries == 5
    assert airbyte_instance.airbyte_waiting_time == 10
    assert airbyte_instance.airbyte_max_time == 7200
    assert airbyte_instance.airbyte_endpoint == "http://localhost:8080"
    assert airbyte_instance.logger is not None
    assert airbyte_instance.airbyte_connection_url == "http://localhost:8080/api/v1/connections/sync"
    assert airbyte_instance.airbyte_job_url == "http://localhost:8080/api/v1/jobs/get"
    assert airbyte_instance.airbyte_connection_metadata_endpoint == "http://localhost:8080/api/v1/connections/get"
    assert airbyte_instance.airbyte_session is not None
    assert airbyte_instance.airbyte_session.headers == {"Content-Type": "application/json", "Accept": "application/json"}
    assert airbyte_instance.airbyte_source_endpoint == "http://localhost:8080/api/v1/sources/get"
    assert airbyte_instance.airbyte_source_update_endpoint == "http://localhost:8080/api/v1/sources/update"

AIRBYTE_JOBS = [
    ({'job': {'id': 51, 'configType': 'sync', 'configId': 'd05daadc-b7ad-4cfc-8078-f3d0a3207d43', 'createdAt': 1641369372, 'updatedAt': 1641369372, 'status': 'pending'}, 'attempts': []}, "pending"),
    ({'job': {'id': 52, 'configType': 'sync', 'configId': 'd05daadc-b7ad-4cfc-8078-f3d0a3207d43', 'createdAt': 1641369372, 'updatedAt': 1641369372, 'status': 'running'}, 'attempts': []}, "running"),
    ({'job': {'id': 53, 'configType': 'sync', 'configId': 'd05daadc-b7ad-4cfc-8078-f3d0a3207d43', 'createdAt': 1641369372, 'updatedAt': 1641369372, 'status': 'success'}, 'attempts': []}, "success")]

@pytest.mark.parametrize("airbyte_job, expected_status", AIRBYTE_JOBS)
def test_process_airbyte_job(airbyte_job, expected_status, airbyte_instance):
    
    status = airbyte_instance.process_airbyte_jobs(airbyte_job)

    assert isinstance(status, AirbyteJobStatus)
    assert status.status == expected_status


AIRBYTE_CONNECTION_SOURCE = [({'connectionId': '396f5f86-2b38-452f-8236-443c19b86841', 'name': 'default', 'namespaceDefinition': 'source', 'namespaceFormat': '${SOURCE_NAMESPACE}', 'prefix': '', 'sourceId': '26297e0f-5b71-428e-b626-09e989873201', 'destinationId': 'f3d18fb4-d644-4c2e-8b49-4c753226b5a9', 'operationIds': ['764a134c-3678-4187-b4a6-4fbcd4c5da1f']}, "26297e0f-5b71-428e-b626-09e989873201")]

@pytest.mark.parametrize("airbyte_connection_source, expected_source_id", AIRBYTE_CONNECTION_SOURCE)
def test_porcess_airbyte_connection_source(airbyte_connection_source, expected_source_id, airbyte_instance):

    source_id = airbyte_instance.process_airbyte_connection_source(airbyte_connection_source)

    assert isinstance(source_id, AirbyteConnectionSource)
    assert source_id.source_id == expected_source_id


AIRBYTE_SOURCE = [({'sourceDefinitionId': '79c1aa37-dae3-42ae-b333-d1c105477715', 'sourceId': '26297e0f-5b71-428e-b626-09e989873201', 'workspaceId': '84775921-0bfc-4702-a633-b815a4fe061c', 'connectionConfiguration': {'subdomain': 'messagebird', 'start_date': '2022-01-01T00:00:00Z', 'auth_method': {'email': 'eric.snyder@messagebird.com', 'api_token': '**********', 'auth_method': 'api_token'}}, 'name': 'zendesk incremental', 'sourceName': 'Zendesk Support'}, {"source_definition": "79c1aa37-dae3-42ae-b333-d1c105477715", "source_id": "26297e0f-5b71-428e-b626-09e989873201", "workspace_id": "84775921-0bfc-4702-a633-b815a4fe061c", "connection_configuration": {"subdomain": "messagebird", "start_date": "2022-01-01T00:00:00Z", "auth_method": {"email": "eric.snyder@messagebird.com","api_token":"**********", "auth_method":"api_token"}}, "source_name": "Zendesk Support", "name": "zendesk incremental"})]

@pytest.mark.parametrize("airbyte_source, expected_source_definition", AIRBYTE_SOURCE)
def test_process_airbyte_source(airbyte_source, expected_source_definition, airbyte_instance):

    airbyte_source = airbyte_instance.process_airbyte_source(airbyte_source)

    assert isinstance(airbyte_source, AirbyteSource)
    assert airbyte_source.source_definition_id == expected_source_definition["source_definition"]
    assert airbyte_source.source_id == expected_source_definition["source_id"]
    assert airbyte_source.workspace_id == expected_source_definition["workspace_id"]
    assert airbyte_source.connection_configuration == expected_source_definition["connection_configuration"]
    assert airbyte_source.name == expected_source_definition["name"]
    assert airbyte_source.source_name == expected_source_definition["source_name"]

    airbyte_mutated_source = airbyte_instance.mutate_source_start_date(airbyte_source, "2020-01-01T00:00:00Z")
    assert isinstance(airbyte_mutated_source, AirbyteSource)
    assert airbyte_mutated_source.connection_configuration["start_date"] == "2020-01-01T00:00:00Z"
