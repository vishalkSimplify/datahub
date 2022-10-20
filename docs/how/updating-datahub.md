# Updating DataHub

This file documents any backwards-incompatible changes in DataHub and assists people when migrating to a new version.

## Next

### Breaking Changes
- Java version 11 or greater is required.
- For any of the GraphQL search queries, the input no longer supports value but instead now accepts a list of values. These values represent an OR relationship where the field value must match any of the values.

### Potential Downtime

### Deprecations

### Other notable Changes

## 0.9.0

### Breaking Changes
- Java version 11 or greater is required.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.45`

### Breaking Changes
- The `getNativeUserInviteToken` and `createNativeUserInviteToken` GraphQL endpoints have been renamed to 
  `getInviteToken` and `createInviteToken` respectively.  Additionally, both now accept an optional `roleUrn` parameter. 
  Both endpoints also now require the `MANAGE_POLICIES` privilege to execute, rather than `MANAGE_USER_CREDENTIALS`
  privilege.
- One of the default policies shipped with DataHub (`urn:li:dataHubPolicy:7`, or `All Users - All Platform Privileges`)
  has been edited to no longer include `MANAGE_POLICIES`. Its name has consequently been changed to
    `All Users - All Platform Privileges (EXCEPT MANAGE POLICIES)`. This change was made to prevent all users from
  effectively acting as superusers by default.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.44`

### Breaking Changes

- Browse Paths have been upgraded to a new format to align more closely with the intention of the feature.
  Learn more about the changes, including steps on upgrading, here: https://datahubproject.io/docs/advanced/browse-paths-upgrade
- The dbt ingestion source's `disable_dbt_node_creation` and `load_schema` options have been removed. They were no longer necessary due to the recently added sibling entities functionality.
- The `snowflake` source now uses newer faster implementation (earlier `snowflake-beta`). Config properties `provision_role` and `check_role_grants` are not supported. Older `snowflake` and `snowflake-usage` are available as `snowflake-legacy` and `snowflake-usage-legacy` sources respectively.

### Potential Downtime

- [Helm] If you're using Helm, please ensure that your version of the `datahub-actions` container is bumped to `v0.0.7` or `head`. 
This version contains changes to support running ingestion in debug mode. Previous versions are not compatible with this release.
Upgrading to helm chart version `0.2.103` will ensure that you have the compatible versions by default. 

### Deprecations

### Other notable Changes

## `v0.8.42`

### Breaking Changes
- Python 3.6 is no longer supported for metadata ingestion
- #5451 `GMS_HOST` and `GMS_PORT` environment variables deprecated in `v0.8.39` have been removed. Use `DATAHUB_GMS_HOST` and `DATAHUB_GMS_PORT` instead.
- #5478 DataHub CLI `delete` command when used with `--hard` option will delete soft-deleted entities which match the other filters given.
- #5471 Looker now populates `userEmail` in dashboard user usage stats. This version of looker connnector will not work with older version of  **datahub-gms** if you have `extract_usage_history` looker config enabled.
- #5529 - `ANALYTICS_ENABLED` environment variable in **datahub-gms** is now deprecated. Use `DATAHUB_ANALYTICS_ENABLED` instead.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.41`

### Breaking Changes
- The `should_overwrite` flag in `csv-enricher` has been replaced with `write_semantics` to match the format used for other sources. See the [documentation](https://datahubproject.io/docs/generated/ingestion/sources/csv/) for more details
- Closing an authorization hole in creating tags adding a Platform Privilege called `Create Tags` for creating tags. This is assigned to `datahub` root user, along 
with default All Users policy. Notice: You may need to add this privilege (or `Manage Tags`) to existing users that need the ability to create tags on the platform. 
- #5329 Below profiling config parameters are now supported in `BigQuery`:
  - profiling.profile_if_updated_since_days (default=1)
  - profiling.profile_table_size_limit (default=1GB)
  - profiling.profile_table_row_limit (default=50000)
  
  Set above parameters to `null` if you want older behaviour.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.40`

### Breaking Changes
- #5240 `lineage_client_project_id` in `bigquery` source is removed. Use `storage_project_id` instead.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.39`

### Breaking Changes
- Refactored the `health` field of the `Dataset` GraphQL Type to be of type **list of HealthStatus** (was type **HealthStatus**). See [this PR](https://github.com/datahub-project/datahub/pull/5222/files) for more details.

### Potential Downtime

### Deprecations
- #4875 Lookml view file contents will no longer be populated in custom_properties, instead view definitions will be always available in the View Definitions tab.
- #5208 `GMS_HOST` and `GMS_PORT` environment variables being set in various containers are deprecated in favour of `DATAHUB_GMS_HOST` and `DATAHUB_GMS_PORT`.
- `KAFKA_TOPIC_NAME` environment variable in **datahub-mae-consumer** and **datahub-gms** is now deprecated. Use `METADATA_AUDIT_EVENT_NAME` instead.
- `KAFKA_MCE_TOPIC_NAME` environment variable in **datahub-mce-consumer** and **datahub-gms** is now deprecated. Use `METADATA_CHANGE_EVENT_NAME` instead.
- `KAFKA_FMCE_TOPIC_NAME` environment variable in **datahub-mce-consumer** and **datahub-gms** is now deprecated. Use `FAILED_METADATA_CHANGE_EVENT_NAME` instead.


### Other notable Changes
- #5132 Profile tables in `snowflake` source only if they have been updated since configured (default: `1`) number of day(s). Update the config `profiling.profile_if_updated_since_days` as per your profiling schedule or set it to `None` if you want older behaviour.

## `v0.8.38`

### Breaking Changes

### Potential Downtime

### Deprecations

### Other notable Changes
- Create & Revoke Access Tokens via the UI
- Create and Manage new users via the UI 
- Improvements to Business Glossary UI
- FIX - Do not require reindexing to migrate to using the UI business glossary 

## `v0.8.36`

### Breaking Changes
- In this release we introduce a brand new Business Glossary experience. With this new experience comes some new ways of indexing data in order to make viewing and traversing the different levels of your Glossary possible. Therefore, you will have to [restore your indices](https://datahubproject.io/docs/how/restore-indices/) in order for the new Glossary experience to work for users that already have existing Glossaries. If this is your first time using DataHub Glossaries, you're all set!

### Potential Downtime

### Deprecations

### Other notable Changes
- #4961 Dropped profiling is not reported by default as that caused a lot of spurious logging in some cases. Set `profiling.report_dropped_profiles` to `True` if you want older behaviour.

## `v0.8.35`

### Breaking Changes

### Potential Downtime

### Deprecations
  - #4875 Lookml view file contents will no longer be populated in custom_properties, instead view definitions will be always available in the View Definitions tab.

### Other notable Changes

## `v0.8.34`

### Breaking Changes
- #4644 Remove `database` option from `snowflake` source which was deprecated since `v0.8.5`
- #4595 Rename confusing config `report_upstream_lineage` to `upstream_lineage_in_report` in `snowflake` connector which was added in `0.8.32`

### Potential Downtime

### Deprecations
- #4644 `host_port` option of `snowflake` and `snowflake-usage` sources deprecated as the name was confusing. Use `account_id` option instead.

### Other notable Changes
- #4760 `check_role_grants` option was added in `snowflake` to disable checking roles in `snowflake` as some people were reporting long run times when checking roles.
