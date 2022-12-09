package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateNameInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getAspectFromEntity;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

@Slf4j
@RequiredArgsConstructor
public class UpdateNameResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final UpdateNameInput input = bindArgument(environment.getArgument("input"), UpdateNameInput.class);
    Urn targetUrn = Urn.createFromString(input.getUrn());
    log.info("Updating name. input: {}", input);

    return CompletableFuture.supplyAsync(() -> {
      if (!_entityService.exists(targetUrn)) {
        throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", targetUrn, targetUrn));
      }

      switch (targetUrn.getEntityType()) {
        case Constants.GLOSSARY_TERM_ENTITY_NAME:
          return updateGlossaryTermName(targetUrn, input, environment.getContext());
        case Constants.GLOSSARY_NODE_ENTITY_NAME:
          return updateGlossaryNodeName(targetUrn, input, environment.getContext());
        case Constants.DOMAIN_ENTITY_NAME:
          return updateDomainName(targetUrn, input, environment.getContext());
        case Constants.CORP_GROUP_ENTITY_NAME:
          return updateGroupName(targetUrn, input, environment.getContext());
        default:
          throw new RuntimeException(
              String.format("Failed to update name. Unsupported resource type %s provided.", targetUrn));
      }
    });
  }

  private Boolean updateGlossaryTermName(
      Urn targetUrn,
      UpdateNameInput input,
      QueryContext context
  ) {
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
    if (GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn)) {
      try {
        GlossaryTermInfo glossaryTermInfo = (GlossaryTermInfo) getAspectFromEntity(
            targetUrn.toString(), Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, _entityService, null);
        if (glossaryTermInfo == null) {
          throw new IllegalArgumentException("Glossary Term does not exist");
        }
        glossaryTermInfo.setName(input.getName());
        Urn actor = UrnUtils.getUrn(context.getActorUrn());
        persistAspect(targetUrn, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, glossaryTermInfo, actor, _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateGlossaryNodeName(
      Urn targetUrn,
      UpdateNameInput input,
      QueryContext context
  ) {
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
    if (GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn)) {
      try {
        GlossaryNodeInfo glossaryNodeInfo = (GlossaryNodeInfo) getAspectFromEntity(
            targetUrn.toString(), Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, _entityService, null);
        if (glossaryNodeInfo == null) {
          throw new IllegalArgumentException("Glossary Node does not exist");
        }
        glossaryNodeInfo.setName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(targetUrn, Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, glossaryNodeInfo, actor, _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateDomainName(
      Urn targetUrn,
      UpdateNameInput input,
      QueryContext context
  ) {
    if (AuthorizationUtils.canManageDomains(context)) {
      try {
        DomainProperties domainProperties = (DomainProperties) getAspectFromEntity(
            targetUrn.toString(), Constants.DOMAIN_PROPERTIES_ASPECT_NAME, _entityService, null);
        if (domainProperties == null) {
          throw new IllegalArgumentException("Domain does not exist");
        }
        domainProperties.setName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(targetUrn, Constants.DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties, actor, _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateGroupName(
          Urn targetUrn,
          UpdateNameInput input,
          QueryContext context
  ) {
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      try {
        CorpGroupInfo corpGroupInfo = (CorpGroupInfo) getAspectFromEntity(
                targetUrn.toString(), Constants.CORP_GROUP_INFO_ASPECT_NAME, _entityService, null);
        if (corpGroupInfo == null) {
          throw new IllegalArgumentException("Group does not exist");
        }
        corpGroupInfo.setDisplayName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(targetUrn, Constants.CORP_GROUP_INFO_ASPECT_NAME, corpGroupInfo, actor, _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
