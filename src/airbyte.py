import logging
import time
from dataclasses import dataclass
from typing import Any
from typing import Dict

import requests
from config import AIRBYTE_ENDPOINT
from requests.adapters import HTTPAdapter


class AirbyteException(Exception): 
    pass


@dataclass
class AirbyteJob:
    job_id: str


@dataclass
class AirbyteJobStatus:
    status: str


@dataclass
class AirbyteConnectionSource:
    source_id: str


@dataclass
class AirbyteSource:
    source_definition_id: str
    source_id: str
    workspace_id: str
    connection_configuration: Any
    source_name: str
    name: str


class AirbyteConnector:
    def __init__(
        self,
        logger: logging.Logger,
        airbyte_connection_id: str,
        airbyte_retries: int,
        airbyte_waiting_time: int,
        airbyte_max_time: int,
        airbyte_source_start_date: str,
        airbyte_endpoint: str = AIRBYTE_ENDPOINT,
    ):

        self.airbyte_connection_id = airbyte_connection_id
        self.airbyte_retries = airbyte_retries
        self.airbyte_waiting_time = airbyte_waiting_time
        self.airbyte_max_time = airbyte_max_time
        self.airbyte_endpoint = airbyte_endpoint
        self.airbyte_source_start_date = airbyte_source_start_date
        self.airbyte_session = requests.Session()
        self.airbyte_session.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        self.logger = logger

    @property
    def airbyte_connection_url(self):
        return f"{self.airbyte_endpoint}/api/v1/connections/sync"

    @property
    def airbyte_job_url(self):
        return f"{self.airbyte_endpoint}/api/v1/jobs/get"

    @property
    def airbyte_connection_metadata_endpoint(self):
        return f"{self.airbyte_endpoint}/api/v1/connections/get"

    @property
    def airbyte_source_endpoint(self):
        return f"{self.airbyte_endpoint}/api/v1/sources/get"

    @property
    def airbyte_source_update_endpoint(self):
        return f"{self.airbyte_endpoint}/api/v1/sources/update"

    @staticmethod
    def process_airbyte_connection(raw_reponse: Dict) -> AirbyteJob:

        return AirbyteJob(raw_reponse["job"]["id"])

    @staticmethod
    def process_airbyte_jobs(raw_reponse: Dict) -> AirbyteJobStatus:

        return AirbyteJobStatus(raw_reponse["job"]["status"])

    @staticmethod
    def process_airbyte_connection_source(raw_reponse: Dict) -> AirbyteConnectionSource:

        return AirbyteConnectionSource(raw_reponse["sourceId"])

    @staticmethod
    def process_airbyte_source(raw_response: Dict) -> AirbyteSource:

        return AirbyteSource(
            source_definition_id=raw_response["sourceDefinitionId"],
            source_id=raw_response["sourceId"],
            workspace_id=raw_response["workspaceId"],
            connection_configuration=raw_response["connectionConfiguration"],
            source_name=raw_response["sourceName"],
            name=raw_response["name"],
        )

    @staticmethod
    def mutate_source_start_date(airbyte_source: AirbyteSource, start_date: str) -> AirbyteSource:

        for key in airbyte_source.connection_configuration:
            if key.endswith("start_date"):
                airbyte_source.connection_configuration[key] = start_date

        return airbyte_source

    def get_connection_source_metadata(self) -> AirbyteConnectionSource:

        data = {"connectionId": self.airbyte_connection_id}

        self.airbyte_session.mount(self.airbyte_connection_metadata_endpoint, HTTPAdapter(max_retries=5))
        response = self.airbyte_session.post(self.airbyte_connection_metadata_endpoint, json=data)

        if response.status_code == 200:
            return self.process_airbyte_connection_source(response.json())

        else:
            raise AirbyteException("Could not get Airbyte Connection Source Metadata")

    def update_airbyte_source(self):

        source_id = self.get_connection_source_metadata().source_id
        data = {"sourceId": source_id}
        self.airbyte_session.mount(self.airbyte_source_endpoint, HTTPAdapter(max_retries=5))
        response = self.airbyte_session.post(self.airbyte_source_endpoint, json=data)

        if response.status_code == 200:
            self.logger.info(f"Airbyte Source Metadata retrieved")
            source_metadata = self.process_airbyte_source(response.json())
            updated_source_metadata = self.mutate_source_start_date(source_metadata, self.airbyte_source_start_date)
            self.airbyte_session.mount(self.airbyte_source_update_endpoint, HTTPAdapter(max_retries=5))
            data = {
                "sourceId": updated_source_metadata.source_id,
                "connectionConfiguration": updated_source_metadata.connection_configuration,
                "name": updated_source_metadata.name,
            }

            response = self.airbyte_session.post(self.airbyte_source_update_endpoint, json=data)
            if response.status_code == 200:
                self.logger.info(f"Airbyte Source Metadata updated")

            else:
                self.logger.error("Unable to update Airbyte Source Metadata")
                raise AirbyteException("Could not update Airbyte Source Metadata")
        else:
            self.logger.info("Unable to update source metadata")
            raise AirbyteException("Could not update source metadata")

    def sync_connection(self) -> AirbyteJob:

        data = {"connectionId": self.airbyte_connection_id}

        response = self.airbyte_session.post(self.airbyte_connection_url, json=data)

        if response.status_code == 200:
            return self.process_airbyte_connection(response.json())

        else:
            retries = 0
            while retries < self.airbyte_retries:
                retries += 1
                response = self.airbyte_session.post(self.airbyte_endpoint, data=data)
                if response.status_code == 200:
                    return self.process_airbyte_connection(response.json())
                else:
                    self.logger.info(f"Retrying in {self.airbyte_waiting_time} seconds, Airbyte Connection Sync")
                    time.sleep(self.airbyte_waiting_time)

            raise AirbyteException(f"Could not connect to Airbyte after {self.airbyte_retries} retries")

    def sync_job_status(self, job_id: str):

        unix_start_time = int(time.time())
        end_time = unix_start_time + self.airbyte_max_time
        data = {"id": job_id}

        while int(time.time()) < end_time:
            response = self.airbyte_session.post(self.airbyte_job_url, json=data)

            if response.status_code == 200:
                job_status = self.process_airbyte_jobs(response.json())
                if job_status.status == "succeeded":
                    self.logger.info(
                        f"Airbyte connection sync succeded, for connection_id {self.airbyte_connection_id} and job_id {job_id}"
                    )
                    return

                elif job_status.status in ("failed", "error", "cancelled"):
                    self.logger.error(
                        f"Airbyte connection sync {job_status.status}, for connection_id {self.airbyte_connection_id} and job_id {job_id}"
                    )
                    raise AirbyteException(
                        f"Airbyte connection sync {job_status.status}, for connection_id {self.airbyte_connection_id} and job_id {job_id}"
                    )

                self.logger.info(f"Airbyte job sync in status {job_status.status}")
                self.logger.info(f"Retrying in {self.airbyte_waiting_time} seconds, Airbyte job Sync")
                time.sleep(self.airbyte_waiting_time)

            else:
                retries = 0
                self.logger.info(f"Unable to sync job status, error code {response.status_code}")
                self.logger.info(f"Retrying in {self.airbyte_waiting_time} seconds, Airbyte job Sync")

                if retries == self.airbyte_retries:
                    raise AirbyteException(f"Unable to sync job status, error code {response.status_code}")

    def run(self):

        if self.airbyte_source_start_date:
            self.logger.info(f"Updating Airbyte Source start date to: {self.airbyte_source_start_date}")
            self.update_airbyte_source()
            self.logger.info(f"Airbyte Source start date updated to: {self.airbyte_source_start_date}")

        self.logger.info(f"Starting Airbyte Connection Sync")
        job = self.sync_connection()
        self.logger.info(f"Airbyte Connection Sync complete")
        self.sync_job_status(job.job_id)
