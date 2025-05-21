# Compute Engine Job Template

This project serves as proof-of-concept implementation of a Compute Engine job for application builders in Vana's Data Access architecture.

## Overview

The worker leverages the test sqlite DB mounted to `/mnt/input/query_results.db` (dir overridable via `INPUT_PATH` env variable) following the [demo application's](https://github.com/vana-com/data-access-demo) schema.

It processes the input data from the `results` table in the database and outputs a `results.json` under `/mnt/output/results.json` (dir overridable via `OUTPUT_PATH` env variable). The `results.json` file will contain a JSON array, where each element is an object representing a row from the `results` table, with keys corresponding to the column names.

## Quick Start

1. Edit the `dummy_data.sql` script with the DLP data refiner schema, seed some dummy data, and add your query at the bottom to simulate the `results` table creation.
2. Run `sqlite3 ./input/query_results.db < dummy_data.sql` to transform the seed data into an SQLite database that can be processed by the job.
3. The `worker.py` by default now relays all columns and rows from the `results` table into `results.json`. You can update it to add any custom processing logic for artifact generation if needed.
4. The worker outputs `results.json` (containing the processed query results) in the output dir `os.getenv("OUTPUT_PATH", "/mnt/output")`. You can modify the worker to output other artifacts as well.
5. Run the `image-build.sh` and `image-run.sh` scripts to test your worker implementation. Make sure to set `DEV_MODE=1` to use the local database file without requiring a real query engine.
6. Run the `image-export.sh` script to generate the `my-compute-job.tar` archive. Gzip this manually or push your changes to main to build a release (with SHA256 checksum).

## Development vs Production Mode

The worker supports two modes of operation:

- **Development Mode**: Set `DEV_MODE=1` to use a local database file without connecting to the query engine. This is useful for testing and development.
- ENV Values for local development when launching worker.py directly:
```
DEV_MODE=1;
INPUT_PATH=../input
OUTPUT_PATH=../output
 ```

# Example: Running in development mode
docker run -e DEV_MODE=1 -v /local/path/to/input:/mnt/input -v /local/path/to/output:/mnt/output my-compute-job
  ```

- **Production Mode**: The default mode connects to the query engine using the `QUERY` and `QUERY_SIGNATURE` environment variables to execute the query first, then processes the results.
  ```
# Example: Running in production mode
docker run -e QUERY="SELECT user_id, locale FROM users" -e QUERY_SIGNATURE="xyz123" -e QUERY_ENGINE_URL="https://query.vana.org" -v /local/path/to/output:/mnt/output my-compute-job
  ```

## Platform Compatibility

**Important**: Docker images must be compatible with AMD architecture to run properly in the Compute Engine's Trusted Execution Environments (TEEs). When building your Docker image:

- Ensure all dependencies and binaries are AMD64-compatible
- Build the Docker image on an AMD64 platform or use the `--platform=linux/amd64` flag with Docker buildx
- Test the image in an AMD64 environment before submission
- Avoid using architecture-specific binaries or libraries when possible

## Utility scripts

These are sugar scripts for docker commands to build, export, and run the worker image consistently for simpler dev cycles / iteration.

The `image-export.sh` script builds an exportable `.tar` for uploading in remote services for registering with the compute engine / image registry contracts.

## Generating test data

The script `dummy_data.sql` can be modified with the relevant schema and dummy data insertion. The query at the bottom of the script file simulates the Query Engine `results` table creation when processing queries.

To transform this dummy data into the input `query_results.db` SQLite DB simply run `sqlite3 ./input/query_results.db < dummy_data.sql`.

*Note:* Only the `results` table will be available in production compute engine jobs. The other tables serve to seed dummy data.

## Building a Compute Job

- Compute Jobs are run as Docker containers inside of the Compute Engine TEE.
- Docker container images ("Compute Instructions") must be approved for a given Data Refiner id by DLP owners through the Compute Instruction Registry smart contract before being submitted for processing via the Compute Engine API.
- The Data Refiner id determines the schema that can be queried against, the granted permissions by the DLP owner, and the cost to access each queried data component (schema, table, column) of the query when running compute jobs.
- Individual queries to the Query Engine are run directly inside of the Compute Job.
- Query results are polled with the `QueryEngineClient` and downloaded as a `query_results.db` SQLite file after successful processing.
  - A queryable `results` table is the only table in the mounted `query_results.db`. This table contains all of the queried data points of the query submitted to the Query Engine through the Compute Job.
  - *Example:*
```sql
-- Refiner Schema:
CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, locale TEXT, zip_code TEXT, city TEXT);

-- Application Builder Query (submitted with the job):
SELECT id, name, locale FROM users;

-- Query Engine outputs `query_results.db` with schema based on the query:
CREATE TABLE results (id INTEGER, name TEXT, locale TEXT);

-- Compute Job processing:
-- The worker.py reads all rows and columns from the `results` table.
-- For the example above, it would effectively perform something like:
-- SELECT * FROM results;
-- And then output this data as a JSON array of objects to /mnt/output/results.json.
```
- Output data Artifacts are provided to the Compute Engine from the Compute Job container through a mounted `/mnt/output` directory.
- Any Artifact files generated in this directory by the Compute Job will later be available for consumption and download by the job owner (=application builder) through the Compute Engine API. The default artifact is `results.json`.

### Example Job Query Result Processing Workflow

1. Retrieve injected input (by the Compute Engine) from environment variables with the `ContainerParams` class.
2. In production mode, query data from the Query Engine with the `QueryEngineClient` helper. The results are downloaded to `query_results.db`. In development mode, a local `query_results.db` is used.
3. The `worker.py` reads all data from the `results` table within the `query_results.db`. By default, it converts all rows and columns into a JSON array. This logic can be customized in `worker.py` for specific transformation, aggregation, or analysis needs.
4. Write generated Artifacts (e.g., `results.json`) to the `/mnt/output` directory for later download by the application builder / job owner wallet through the Compute Engine API.

### Submitting Compute Instructions For DLP Approval

1. Build and export the Compute Job Docker image to a `.tar`, and `gzip` to a `.tar.gz`.
2. Upload it to a publicly accessible URL for later retrieval by the Compute Engine.
3. Calculate the SHA256 checksum of the image archive file and document for use in on-chain registration. (*Example:* `sha256sum my-compute-job.tar.gz | cut -d' ' -f1`)
4. Write the Compute Instruction on-chain to the ComputeInstructionRegistry smart contract via the `addComputeInstruction` function with both the publicly available image URL and the SHA256 image checksum.
5. Notify the relevant DLP owner for Compute Instruction image audits and eventual approval with the DLP owner wallet through the `updateComputeInstruction` ComputeInstructionRegistry smart contract function.
6. Approval can be checked and verified on-chain with the `isApproved` ComputeInstructionRegistry smart contract function.
```

**Regarding `output/results.json`:**

The `output/results.json` file provided in the initial prompt:
```json
{
    "u001": "en_US",
    "u002": "en_GB",
    "u003": "fr_FR",
    "u004": "de_DE",
    "u005": "es_ES",
    "u006": "it_IT",
    "u007": "pt_BR",
    "u008": "nl_NL",
    "u009": "ru_RU",
    "u010": "ja_JP"
}
```
was the output of the *previous* version of `worker.py`.

With the **new** `worker.py` (the one I provided in the previous response) and the `dummy_data.sql` that creates a `results` table like:
```sql
CREATE TABLE results AS
SELECT
    *
FROM users;
```
the `output/results.json` file would now look like this:
```json
[
    {
        "user_id": "u001",
        "locale": "en_US"
    },
    {
        "user_id": "u002",
        "locale": "en_GB"
    },
    {
        "user_id": "u003",
        "locale": "fr_FR"
    },
    {
        "user_id": "u004",
        "locale": "de_DE"
    },
    {
        "user_id": "u005",
        "locale": "es_ES"
    },
    {
        "user_id": "u006",
        "locale": "it_IT"
    },
    {
        "user_id": "u007",
        "locale": "pt_BR"
    },
    {
        "user_id": "u008",
        "locale": "nl_NL"
    },
    {
        "user_id": "u009",
        "locale": "ru_RU"
    },
    {
        "user_id": "u010",
        "locale": "ja_JP"
    }
]
```