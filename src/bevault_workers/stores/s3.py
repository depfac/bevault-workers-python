import boto3
import posixpath
from urllib.parse import urlparse
from .base_store import FileStore
from .store_utils import get_first


class Store(FileStore):
    def __init__(self, config):
        self.bucket = get_first(config, "bucketName")
        self.prefix = get_first(config, "prefix") or ""
        access_key = get_first(config, "accessKey")
        secret_key = get_first(config, "secretKey")
        service_url = get_first(config, "serviceUrl")
        region_name = get_first(config, "regionEndPoint")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=service_url or None,
            region_name=region_name or None,
        )

    def connect(self):
        pass  # boto3 handles this internally

    def createFileToken(self, filename):
        """Create a filetoken without including the configured prefix.
        
        The prefix is stored in the store configuration and will be added
        internally when the filetoken is used.
        """
        return f"s3://{self.bucket}/{filename}"

    def listFiles(self, prefix="", suffix=""):
        paginator = self.client.get_paginator("list_objects_v2")
        result = []
        search_prefix = self.prefix + prefix if self.prefix else prefix
        for page in paginator.paginate(Bucket=self.bucket, Prefix=search_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Remove configured prefix from key for token (prefix is in config, not token)
                relative_key = self._remove_prefix_from_path(key)
                if relative_key.endswith(suffix):
                    result.append(relative_key)
        return result

    def getFileName(self, fileToken):
        filepath = self._extract_filepath_from_token(fileToken)
        return posixpath.basename(filepath) if filepath else ""

    def openRead(self, fileToken):
        parsed = urlparse(fileToken)
        filepath = self._extract_filepath_from_token(fileToken)
        key = self._add_prefix_to_path(filepath)
        response = self.client.get_object(Bucket=parsed.netloc, Key=key)
        return response["Body"].read()

    def openWrite(self, fileToken, content: bytes):
        parsed = urlparse(fileToken)
        filepath = self._extract_filepath_from_token(fileToken)
        key = self._add_prefix_to_path(filepath)
        self.client.put_object(Bucket=parsed.netloc, Key=key, Body=content)

    def delete(self, fileToken):
        parsed = urlparse(fileToken)
        filepath = self._extract_filepath_from_token(fileToken)
        key = self._add_prefix_to_path(filepath)
        self.client.delete_object(Bucket=parsed.netloc, Key=key)

    def exists(self, fileToken):
        parsed = urlparse(fileToken)
        filepath = self._extract_filepath_from_token(fileToken)
        key = self._add_prefix_to_path(filepath)
        try:
            self.client.head_object(Bucket=parsed.netloc, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False
