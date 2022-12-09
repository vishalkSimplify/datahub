import React from 'react';
import { EntityType, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    count,
    insights,
    logoComponent,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    count?: number | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type="Domain"
            typeIcon={entityRegistry.getIcon(EntityType.Domain, 14, IconStyleType.ACCENT)}
            owners={owners}
            insights={insights}
            logoComponent={logoComponent}
            entityCount={count || undefined}
        />
    );
};
