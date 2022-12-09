import React, { useEffect, useState } from 'react';
import { Button, Empty, List, Pagination, Typography } from 'antd';
import { useLocation } from 'react-router';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { PlusOutlined } from '@ant-design/icons';
import { Domain } from '../../types.generated';
import { useListDomainsQuery } from '../../graphql/domain.generated';
import CreateDomainModal from './CreateDomainModal';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import DomainListItem from './DomainListItem';
import { SearchBar } from '../search/SearchBar';
import { useEntityRegistry } from '../useEntityRegistry';
import { scrollToTop } from '../shared/searchUtils';
import { addToListDomainsCache, removeFromListDomainsCache } from './utils';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID } from '../onboarding/config/DomainsOnboardingConfig';

const DomainsContainer = styled.div``;

const DomainsStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const DomainsPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 12px;
    padding-left: 16px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const DEFAULT_PAGE_SIZE = 25;

export const DomainsList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, client, refetch } = useListDomainsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const totalDomains = data?.listDomains?.total || 0;
    const lastResultIndex = start + pageSize > totalDomains ? totalDomains : start + pageSize;
    const domains = data?.listDomains?.domains || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListDomainsCache(client, urn, page, pageSize, query);
        setTimeout(function () {
            refetch?.();
        }, 2000);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && <Message type="error" content="Failed to load domains! An unexpected error occurred." />}
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <DomainsContainer>
                <TabToolbar>
                    <Button id={DOMAINS_CREATE_DOMAIN_ID} type="text" onClick={() => setIsCreatingDomain(true)}>
                        <PlusOutlined /> New Domain
                    </Button>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search domains..."
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => setQuery(q)}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <DomainsStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Domains!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={domains}
                    renderItem={(item: any) => (
                        <DomainListItem
                            key={item.urn}
                            domain={item as Domain}
                            onDelete={() => handleDelete(item.urn)}
                        />
                    )}
                />
                <DomainsPaginationContainer>
                    <PaginationInfo>
                        <b>
                            {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                        </b>
                        of <b>{totalDomains}</b>
                    </PaginationInfo>
                    <Pagination
                        current={page}
                        pageSize={pageSize}
                        total={totalDomains}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                    <span />
                </DomainsPaginationContainer>
                {isCreatingDomain && (
                    <CreateDomainModal
                        onClose={() => setIsCreatingDomain(false)}
                        onCreate={(urn, _, name, description) => {
                            addToListDomainsCache(
                                client,
                                {
                                    urn,
                                    properties: {
                                        name,
                                        description,
                                    },
                                    ownership: null,
                                    entities: null,
                                },
                                pageSize,
                                query,
                            );
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                )}
            </DomainsContainer>
        </>
    );
};
