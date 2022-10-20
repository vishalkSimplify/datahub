from dataclasses import dataclass
from typing import Iterable, Union, overload

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import UsageAggregationClass


@dataclass
class MetadataWorkUnit(WorkUnit):
    metadata: Union[
        MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
    ]
    # A workunit creator can determine if this workunit is allowed to fail
    treat_errors_as_warnings: bool = False

    @overload
    def __init__(self, id: str, mce: MetadataChangeEvent):
        # TODO: Force `mce` to be a keyword-only argument.
        ...

    @overload
    def __init__(self, id: str, *, mcp_raw: MetadataChangeProposal):
        # We force `mcp_raw` to be a keyword-only argument.
        ...

    @overload
    def __init__(
        self,
        id: str,
        *,
        mcp: MetadataChangeProposalWrapper,
        treat_errors_as_warnings: bool = False,
    ):
        # We force `mcp` to be a keyword-only argument.
        ...

    def __init__(
        self,
        id: str,
        mce: MetadataChangeEvent = None,
        mcp: MetadataChangeProposalWrapper = None,
        mcp_raw: MetadataChangeProposal = None,
        treat_errors_as_warnings: bool = False,
    ):
        super().__init__(id)
        self.treat_errors_as_warnings = treat_errors_as_warnings

        if sum(1 if v else 0 for v in [mce, mcp, mcp_raw]) != 1:
            raise ValueError("exactly one of mce, mcp, or mcp_raw must be provided")

        if mcp_raw:
            assert not (mce or mcp)
            self.metadata = mcp_raw
        elif mce and not mcp:
            self.metadata = mce
        elif mcp and not mce:
            self.metadata = mcp

    def get_metadata(self) -> dict:
        return {"metadata": self.metadata}

    def get_urn(self) -> str:
        if isinstance(self.metadata, MetadataChangeEvent):
            return self.metadata.proposedSnapshot.urn
        else:
            assert self.metadata.entityUrn
            return self.metadata.entityUrn

    def decompose_mce_into_mcps(self) -> Iterable["MetadataWorkUnit"]:
        from datahub.emitter.mcp_builder import mcps_from_mce

        assert isinstance(self.metadata, MetadataChangeEvent)

        yield from [
            MetadataWorkUnit(
                id=self.id,
                mcp=mcpw,
                treat_errors_as_warnings=self.treat_errors_as_warnings,
            )
            for mcpw in mcps_from_mce(self.metadata)
        ]


@dataclass
class UsageStatsWorkUnit(WorkUnit):
    usageStats: UsageAggregationClass

    def get_metadata(self) -> dict:
        return {"usage": self.usageStats}
