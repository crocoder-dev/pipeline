import { getMergeRequestCommits, type Context, type GetMergeRequestCommitsEntities, type GetMergeRequestCommitsSourceControl } from "@dxta/extract-functions";
import { GitHubSourceControl, GitlabSourceControl } from "@dxta/source-control";
import { mergeRequestCommits, namespaces, repositories, mergeRequests, RepositorySchema, NamespaceSchema, MergeRequestSchema } from "@dxta/extract-schema";
import { EventHandler } from "@stack/config/create-event";
import { extractMergeRequestsEvent } from "./events";
import { createMessageHandler } from "@stack/config/create-message";
import { MessageKind, metadataSchema } from "./messages";
import { z } from "zod";
import { getClerkUserToken } from "./get-clerk-user-token";
import { insertEvent } from "@dxta/crawl-functions";
import { events } from "@dxta/crawl-schema";
import { getTenantDb, type OmitDb } from "@stack/config/get-tenant-db";

export const mrcsh = createMessageHandler({
  queueId: 'ExtractQueue',
  kind: MessageKind.MergeRequestCommit,
  metadataShape: metadataSchema.shape,
  contentShape: z.object({
    mergeRequestId: MergeRequestSchema.shape.id,
    repositoryId: RepositorySchema.shape.id,
    namespaceId: NamespaceSchema.shape.id,
  }).shape,
  handler: async (message) => {
    if (!message) {
      console.warn("Expected message to have content,but get empty")
      return;
    }

    context.integrations.sourceControl = await initSourceControl(message.metadata.userId, message.metadata.sourceControl);

    const { mergeRequestId, namespaceId, repositoryId } = message.content;

    await getMergeRequestCommits({
        mergeRequestId,
        namespaceId,
        repositoryId
      }, { ...context, db: getTenantDb(message.metadata.tenantId) })
  }
});

const { sender } = mrcsh;

  const context: OmitDb<Context<
    GetMergeRequestCommitsSourceControl,
    GetMergeRequestCommitsEntities
  >> = {
  entities: {
    mergeRequestCommits,
    namespaces,
    repositories,
    mergeRequests,
  },
  integrations: {
    sourceControl: null,
  },
};


const initSourceControl = async (userId: string, sourceControl: 'github' | 'gitlab') => {
  const accessToken = await getClerkUserToken(userId, `oauth_${sourceControl}`);
  if (sourceControl === 'github') return new GitHubSourceControl({ auth: accessToken });
  if (sourceControl === 'gitlab') return new GitlabSourceControl(accessToken);
  return null;
}

export const eventHandler = EventHandler(extractMergeRequestsEvent, async (ev) => {
  const { mergeRequestIds, namespaceId, repositoryId } = ev.properties;

  const { sourceControl, userId } = ev.metadata;


  const arrayOfExtractMergeRequestData = [];
  for (let i = 0; i < mergeRequestIds.length; i += 1) {
    arrayOfExtractMergeRequestData.push({
      mergeRequestId: mergeRequestIds[i]!,
      namespaceId,
      repositoryId,
    })
  }

  await insertEvent(
    { crawlId: ev.metadata.crawlId, eventNamespace: 'mergeRequestCommit', eventDetail: 'crawlInfo', data: { calls: mergeRequestIds.length } },
    { db: getTenantDb(ev.metadata.tenantId), entities: { events } }
  );

  await sender.sendAll(arrayOfExtractMergeRequestData, {
    crawlId: ev.metadata.crawlId,
    version: 1,
    caller: 'extract-merge-request-commits',
    sourceControl,
    userId,
    timestamp: new Date().getTime(),
    from: ev.metadata.from,
    to: ev.metadata.to,
    tenantId: ev.metadata.tenantId,
  })

});

