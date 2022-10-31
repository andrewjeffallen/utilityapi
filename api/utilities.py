import os
import json
from typing import Dict, List, Any, Optional
import requests
from requests import Response
import pandas as pd

from io import StringIO

Response = requests.models.Response

API_AUTH_TOKEN = os.environ["API_AUTH_TOKEN"]


class UtililtyAPIBase:
    """Base client to connect to Utility API"""

    def __init__(self, api_token: str = API_AUTH_TOKEN):
        self.base_url = "https://utilityapi.com/api/v2"
        self.base_payload = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

    def get(self, data_name: str) -> Response:
        """
        Base method to get request at base_url

        Args:
            data_name (str): The target report for request
        """
        resp: Response = requests.get(
            f"{self.base_url}/{data_name}", headers=self.base_payload
        )

        resp.raise_for_status()
        return resp


class Meters(UtililtyAPIBase):
    def __init__(self, api_token: str = API_AUTH_TOKEN):
        super().__init__(api_token)

        self.data_name = "meters"

    def get_active_meters(self) -> List:
        """Method to retrieve active, authorized meters from Utility

        Returns:
            List: list of active, authorized meters
        """
        resp = self.get(self.data_name)
        download = json.loads(resp.text)

        active_meters = [
            i["uid"] for i in download[self.data_name] if i["is_activated"]
        ]
        return active_meters


class Files(UtililtyAPIBase):
    """
    Class for file retrieval including Bills and Intervals data
    """

    def __init__(self, api_token: str = API_AUTH_TOKEN):
        super().__init__(api_token)

        self.bill_files_url = "files/meters_bills_csv?meters"
        self.interval_files_url = "files/intervals_csv?meters"

    def get_bills_dataframe(self, meter_uid: int) -> pd.DataFrame:
        """_summary_

        Args:
            meter_uid (int): _description_

        Returns:
            pd.DataFrame: _description_
        """

        data_name = f"{self.bill_files_url}={meter_uid}"

        download = self.get(data_name=data_name).content
        return pd.read_csv(StringIO(download.decode("utf-8")), error_bad_lines=False)

    def get_intervals_dataframe(self, meter_uid: int) -> pd.DataFrame:
        """_summary_

        Args:
            meter_uid (int): _description_

        Returns:
            pd.DataFrame: _description_
        """

        data_name = f"{self.interval_files_url}={meter_uid}"

        download = self.get(data_name=data_name).content
        return pd.read_csv(StringIO(download.decode("utf-8")), error_bad_lines=False)
