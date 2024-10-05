# Iceberg + Nessie + S3

1. Clone the projectnessie/nessie repo

1. Navigate to the `docker` directory and run the following command:

   ```bash
   docker compose -f catalog-auth-s3/docker-compose.yml up
   ```

1. Get a token from the auth service:

   ```bash
   token=$(curl http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token --user client1:s3cr3t -d 'grant_type=client_credentials' -d 'scope=profile' | jq -r .access_token)
   ```

1. Use the token to query the API:

   ```console
   $ curl http://127.0.0.1:19120/api/v2/config -H "Authorization: Bearer $token"
   {
     "defaultBranch" : "main",
     "minSupportedApiVersion" : 1,
     "maxSupportedApiVersion" : 2,
     "actualApiVersion" : 2,
     "specVersion" : "2.2.0",
     "noAncestorHash" : "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
     "repositoryCreationTimestamp" : "2024-09-29T02:08:47.562244091Z",
     "oldestPossibleCommitTimestamp" : "2024-09-29T02:08:47.562244091Z"
   }
   ```

1. Configure $HOME/.pyiceberg.yaml

   ```yaml
   catalog:
     rest:
       uri: http://127.0.0.1:19120/iceberg
       token: <token from a previous step>
       py-io-impl: pyiceberg.io.fsspec.FsspecFileIO
       access-delegation: remote-signing  # https://github.com/apache/iceberg-python/pull/1033/files
       s3.signer: S3V4RestSigner  # https://github.com/apache/iceberg-python/commit/ceeb08435c019859b9a2b8d6c2d36758d989ff51
   ```

1. Download sample data

   ```
   curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet -o ./yellow_tripdata_2023-01.parquet
   ```

1. Run the script to load data into the catalog:

   ```bash
   uv run python connect.py
   ```
