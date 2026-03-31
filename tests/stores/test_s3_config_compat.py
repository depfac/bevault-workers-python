from unittest.mock import MagicMock, patch

from bevault_workers.stores.s3 import Store


@patch("bevault_workers.stores.s3.boto3.client")
def test_s3_keeps_legacy_configuration_keys(mock_boto_client):
    mock_boto_client.return_value = MagicMock()

    Store(
        {
            "BucketName": "legacy-bucket",
            "AccessKey": "ak",
            "SecretKey": "sk",
            "ServiceUrl": "http://localhost:9000",
            "Prefix": "legacy-prefix/",
        }
    )

    mock_boto_client.assert_called_once_with(
        "s3",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
        endpoint_url="http://localhost:9000",
        region_name=None,
    )


@patch("bevault_workers.stores.s3.boto3.client")
def test_s3_supports_states_camel_case_with_region_endpoint(mock_boto_client):
    mock_boto_client.return_value = MagicMock()

    store = Store(
        {
            "bucketName": "states-bucket",
            "accessKey": "states-ak",
            "secretKey": "states-sk",
            "regionEndPoint": "us-east-1",
            "prefix": "states-prefix/",
        }
    )

    assert store.bucket == "states-bucket"
    assert store.prefix == "states-prefix/"
    mock_boto_client.assert_called_once_with(
        "s3",
        aws_access_key_id="states-ak",
        aws_secret_access_key="states-sk",
        endpoint_url=None,
        region_name="us-east-1",
    )


@patch("bevault_workers.stores.s3.boto3.client")
def test_s3_service_url_takes_precedence_over_region_endpoint(mock_boto_client):
    mock_boto_client.return_value = MagicMock()

    Store(
        {
            "BucketName": "bucket",
            "AccessKey": "ak",
            "SecretKey": "sk",
            "ServiceUrl": "http://minio:9000",
            "RegionEndPoint": "us-east-1",
        }
    )

    call_kwargs = mock_boto_client.call_args.kwargs
    assert call_kwargs["endpoint_url"] == "http://minio:9000"
    assert call_kwargs["region_name"] == "us-east-1"
