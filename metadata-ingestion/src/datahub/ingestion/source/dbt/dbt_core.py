import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import dateutil.parser
import requests
from pydantic import BaseModel, Field, validator

from datahub.configuration.github import GitHubReference
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTCommonConfig,
    DBTNode,
    DBTSourceBase,
    DBTSourceReport,
    DBTTest,
    DBTTestResult,
)

logger = logging.getLogger(__name__)


class DBTCoreConfig(DBTCommonConfig):
    manifest_path: str = Field(
        description="Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json Note this can be a local file or a URI."
    )
    catalog_path: str = Field(
        description="Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json Note this can be a local file or a URI."
    )
    sources_path: Optional[str] = Field(
        default=None,
        description="Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. Note this can be a local file or a URI.",
    )
    test_results_path: Optional[str] = Field(
        default=None,
        description="Path to output of dbt test run as run_results file in JSON format. See https://docs.getdbt.com/reference/artifacts/run-results-json. If not specified, test execution results will not be populated in DataHub.",
    )

    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="When fetching manifest files from s3, configuration for aws connection details",
    )

    github_info: Optional[GitHubReference] = Field(
        None,
        description="Reference to your github location to enable easy navigation from DataHub to your dbt files.",
    )

    @property
    def s3_client(self):
        assert self.aws_connection
        return self.aws_connection.get_s3_client()

    @validator("aws_connection")
    def aws_connection_needed_if_s3_uris_present(
        cls, aws_connection: Optional[AwsConnectionConfig], values: Dict, **kwargs: Any
    ) -> Optional[AwsConnectionConfig]:
        # first check if there are fields that contain s3 uris
        uri_containing_fields = [
            f
            for f in [
                "manifest_path",
                "catalog_path",
                "sources_path",
                "test_results_path",
            ]
            if (values.get(f) or "").startswith("s3://")
        ]

        if uri_containing_fields and not aws_connection:
            raise ValueError(
                f"Please provide aws_connection configuration, since s3 uris have been provided in fields {uri_containing_fields}"
            )
        return aws_connection


def get_columns(
    catalog_node: dict, manifest_node: dict, tag_prefix: str
) -> List[DBTColumn]:
    columns = []

    catalog_columns = catalog_node["columns"]
    manifest_columns = manifest_node.get("columns", {})

    for key, catalog_column in catalog_columns.items():
        manifest_column = manifest_columns.get(key.lower(), {})

        meta = manifest_column.get("meta", {})

        tags = manifest_column.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        dbtCol = DBTColumn(
            name=catalog_column["name"].lower(),
            comment=catalog_column.get("comment", ""),
            description=manifest_column.get("description", ""),
            data_type=catalog_column["type"],
            index=catalog_column["index"],
            meta=meta,
            tags=tags,
        )
        columns.append(dbtCol)
    return columns


def extract_dbt_entities(
    all_manifest_entities: Dict[str, Dict[str, Any]],
    all_catalog_entities: Dict[str, Dict[str, Any]],
    sources_results: List[Dict[str, Any]],
    manifest_adapter: str,
    use_identifiers: bool,
    tag_prefix: str,
    report: DBTSourceReport,
) -> List[DBTNode]:
    sources_by_id = {x["unique_id"]: x for x in sources_results}

    dbt_entities = []
    for key, manifest_node in all_manifest_entities.items():
        name = manifest_node["name"]

        if "identifier" in manifest_node and use_identifiers:
            name = manifest_node["identifier"]

        if (
            manifest_node.get("alias") is not None
            and manifest_node.get("resource_type")
            != "test"  # tests have non-human-friendly aliases, so we don't want to use it for tests
        ):
            name = manifest_node["alias"]

        # initialize comment to "" for consistency with descriptions
        # (since dbt null/undefined descriptions as "")
        comment = ""

        if key in all_catalog_entities and all_catalog_entities[key]["metadata"].get(
            "comment"
        ):
            comment = all_catalog_entities[key]["metadata"]["comment"]

        materialization = None
        if "materialized" in manifest_node.get("config", {}):
            # It's a model
            materialization = manifest_node["config"]["materialized"]

        upstream_nodes = []
        if "depends_on" in manifest_node:
            upstream_nodes = manifest_node["depends_on"]["nodes"]

        # It's a source
        catalog_node = all_catalog_entities.get(key)
        catalog_type = None

        if catalog_node is None:
            if materialization != "test":
                report.report_warning(
                    key,
                    f"Entity {key} ({name}) is in manifest but missing from catalog",
                )
        else:
            catalog_type = all_catalog_entities[key]["metadata"]["type"]

        query_tag_props = manifest_node.get("query_tag", {})

        meta = manifest_node.get("meta", {})

        owner = meta.get("owner")
        if owner is None:
            owner = manifest_node.get("config", {}).get("meta", {}).get("owner")

        tags = manifest_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]
        if not meta:
            meta = manifest_node.get("config", {}).get("meta", {})

        max_loaded_at_str = sources_by_id.get(key, {}).get("max_loaded_at")
        max_loaded_at = None
        if max_loaded_at_str:
            max_loaded_at = dateutil.parser.parse(max_loaded_at_str)

        test_info = None
        if manifest_node.get("resource_type") == "test":
            test_metadata = manifest_node.get("test_metadata", {})
            kw_args = test_metadata.get("kwargs", {})

            qualified_test_name = (
                (test_metadata.get("namespace") or "")
                + "."
                + (test_metadata.get("name") or "")
            )
            qualified_test_name = (
                qualified_test_name[1:]
                if qualified_test_name.startswith(".")
                else qualified_test_name
            )
            test_info = DBTTest(
                qualified_test_name=qualified_test_name,
                column_name=kw_args.get("column_name"),
                kw_args=kw_args,
            )

        dbtNode = DBTNode(
            dbt_name=key,
            dbt_adapter=manifest_adapter,
            database=manifest_node["database"],
            schema=manifest_node["schema"],
            name=name,
            alias=manifest_node.get("alias"),
            dbt_file_path=manifest_node["original_file_path"],
            node_type=manifest_node["resource_type"],
            max_loaded_at=max_loaded_at,
            comment=comment,
            description=manifest_node.get("description", ""),
            raw_code=manifest_node.get(
                "raw_code", manifest_node.get("raw_sql")
            ),  # Backward compatibility dbt <=v1.2
            language=manifest_node.get(
                "language", "sql"
            ),  # Backward compatibility dbt <=v1.2
            upstream_nodes=upstream_nodes,
            materialization=materialization,
            catalog_type=catalog_type,
            meta=meta,
            query_tag=query_tag_props,
            tags=tags,
            owner=owner,
            compiled_code=manifest_node.get(
                "compiled_code", manifest_node.get("compiled_sql")
            ),  # Backward compatibility dbt <=v1.2
            test_info=test_info,
        )

        # Load columns from catalog, and override some properties from manifest.
        if dbtNode.materialization not in [
            "ephemeral",
            "test",
        ]:
            logger.debug(f"Loading schema info for {dbtNode.dbt_name}")
            if catalog_node is not None:
                # We already have done the reporting for catalog_node being None above.
                dbtNode.columns = get_columns(catalog_node, manifest_node, tag_prefix)

        else:
            dbtNode.columns = []

        dbt_entities.append(dbtNode)

    return dbt_entities


class DBTRunTiming(BaseModel):
    name: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class DBTRunResult(BaseModel):
    class Config:
        extra = "allow"

    status: str
    timing: List[DBTRunTiming] = []
    unique_id: str
    failures: Optional[int] = None
    message: Optional[str] = None


class DBTRunMetadata(BaseModel):
    dbt_schema_version: str
    dbt_version: str
    generated_at: str
    invocation_id: str


def load_test_results(
    config: DBTCommonConfig,
    test_results_json: Dict[str, Any],
    all_nodes: List[DBTNode],
) -> List[DBTNode]:
    dbt_metadata = DBTRunMetadata.parse_obj(test_results_json.get("metadata", {}))

    test_nodes_map: Dict[str, DBTNode] = {
        x.dbt_name: x for x in all_nodes if x.node_type == "test"
    }

    results = test_results_json.get("results", [])
    for result in results:
        run_result = DBTRunResult.parse_obj(result)
        id = run_result.unique_id

        if not id.startswith("test."):
            continue

        test_node = test_nodes_map.get(id)
        if not test_node:
            logger.debug(f"Failed to find test node {id} in the catalog")
            continue

        if run_result.status == "success":
            # This was probably a docs generate run result, so this isn't actually
            # a test result.
            continue

        if run_result.status != "pass":
            native_results = {"message": run_result.message or ""}
            if run_result.failures:
                native_results.update({"failures": str(run_result.failures)})
        else:
            native_results = {}

        stage_timings = {x.name: x.started_at for x in run_result.timing}
        # look for execution start time, fall back to compile start time and finally generation time
        execution_timestamp = (
            stage_timings.get("execute")
            or stage_timings.get("compile")
            or dbt_metadata.generated_at
        )

        execution_timestamp_parsed = datetime.strptime(
            execution_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        test_result = DBTTestResult(
            invocation_id=dbt_metadata.invocation_id,
            status=run_result.status,
            native_results=native_results,
            execution_time=execution_timestamp_parsed,
        )

        assert test_node.test_info is not None
        assert test_node.test_result is None
        test_node.test_result = test_result

    return all_nodes


@platform_name("dbt")
@config_class(DBTCoreConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "", supported=False)
class DBTCoreSource(DBTSourceBase):
    """
    The artifacts used by this source are:
    - [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
      - This file contains model, source, tests and lineage data.
    - [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
      - This file contains schema data.
      - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
    - [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
      - This file contains metadata for sources with freshness checks.
      - We transfer dbt's freshness checks to DataHub's last-modified fields.
      - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
    - [dbt run_results file](https://docs.getdbt.com/reference/artifacts/run-results-json)
      - This file contains metadata from the result of a dbt run, e.g. dbt test
      - When provided, we transfer dbt test run results into assertion run events to see a timeline of test runs on the dataset
    """

    config: DBTCoreConfig

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    def load_file_as_json(self, uri: str) -> Any:
        if re.match("^https?://", uri):
            return json.loads(requests.get(uri).text)
        elif re.match("^s3://", uri):
            u = urlparse(uri)
            response = self.config.s3_client.get_object(
                Bucket=u.netloc, Key=u.path.lstrip("/")
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        else:
            with open(uri, "r") as f:
                return json.load(f)

    def loadManifestAndCatalog(
        self,
        manifest_path: str,
        catalog_path: str,
        sources_path: Optional[str],
        use_identifiers: bool,
        tag_prefix: str,
    ) -> Tuple[
        List[DBTNode],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        dbt_manifest_json = self.load_file_as_json(manifest_path)

        dbt_catalog_json = self.load_file_as_json(catalog_path)

        if sources_path is not None:
            dbt_sources_json = self.load_file_as_json(sources_path)
            sources_results = dbt_sources_json["results"]
        else:
            sources_results = {}

        manifest_schema = dbt_manifest_json["metadata"].get("dbt_schema_version")
        manifest_version = dbt_manifest_json["metadata"].get("dbt_version")
        manifest_adapter = dbt_manifest_json["metadata"].get("adapter_type")

        catalog_schema = dbt_catalog_json.get("metadata", {}).get("dbt_schema_version")
        catalog_version = dbt_catalog_json.get("metadata", {}).get("dbt_version")

        manifest_nodes = dbt_manifest_json["nodes"]
        manifest_sources = dbt_manifest_json["sources"]

        all_manifest_entities = {**manifest_nodes, **manifest_sources}

        catalog_nodes = dbt_catalog_json["nodes"]
        catalog_sources = dbt_catalog_json["sources"]

        all_catalog_entities = {**catalog_nodes, **catalog_sources}

        nodes = extract_dbt_entities(
            all_manifest_entities,
            all_catalog_entities,
            sources_results,
            manifest_adapter,
            use_identifiers,
            tag_prefix,
            self.report,
        )

        return (
            nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
        )

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        (
            all_nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
        ) = self.loadManifestAndCatalog(
            self.config.manifest_path,
            self.config.catalog_path,
            self.config.sources_path,
            self.config.use_identifiers,
            self.config.tag_prefix,
        )
        additional_custom_props = {
            "manifest_schema": manifest_schema,
            "manifest_version": manifest_version,
            "manifest_adapter": manifest_adapter,
            "catalog_schema": catalog_schema,
            "catalog_version": catalog_version,
        }

        if self.config.test_results_path:
            # This will populate the test_results field on each test node.
            all_nodes = load_test_results(
                self.config,
                self.load_file_as_json(self.config.test_results_path),
                all_nodes,
            )

        return all_nodes, additional_custom_props

    def get_external_url(self, node: DBTNode) -> Optional[str]:
        if self.config.github_info and node.dbt_file_path:
            return self.config.github_info.get_url_for_file_path(node.dbt_file_path)
        return None

    def get_platform_instance_id(self) -> str:
        """The DBT project identifier is used as platform instance."""

        project_id = (
            self.load_file_as_json(self.config.manifest_path)
            .get("metadata", {})
            .get("project_id")
        )
        if project_id is None:
            raise ValueError("DBT project identifier is not found in manifest")

        return f"{self.platform}_{project_id}"
