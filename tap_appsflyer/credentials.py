import boto3


class Config:
    def __init__(self):
        self.ssm_client = boto3.client('ssm')
        self.access_token = self._get_access_token()

    def _get_access_token(self):
        return self.ssm_client.get_parameter(Name='/Appsflyer/token', WithDecryption=True)['Parameter']['Value']




