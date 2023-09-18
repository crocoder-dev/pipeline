import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { Config } from "sst/node/config";
import { EventHandler } from "sst/node/event-bus";

import {
  getMergeRequests,
  type Context,
  type GetMergeRequestsEntities,
  type GetMergeRequestsSourceControl,
} from "@acme/extract-functions";
import { NamespaceSchema, RepositorySchema, mergeRequests, namespaces, repositories } from "@acme/extract-schema";
import { GitHubSourceControl, GitlabSourceControl } from "@acme/source-control";

import { extractMergeRequestsEvent, extractRepositoryEvent } from "./events";
import { eq } from "drizzle-orm";
import { MessageKind, metadataSchema, paginationSchema, timePeriodSchema } from "./messages";
import { z } from "zod";
import { createMessageHandler } from "./create-message";
import { getClerkUserToken } from "./get-clerk-user-token";


export const mergeRequestSenderHandler = createMessageHandler({
  kind: MessageKind.MergeRequest,
  metadataShape: metadataSchema.shape,
  contentShape: z.object({
    repository: RepositorySchema,
    namespace: NamespaceSchema,
    pagination: paginationSchema,
    timePeriod: timePeriodSchema,
  }).shape,
  handler: async (message) => {

    if (!message) {
      console.warn("Expected message to have content,but get empty")
      return;
    }

    context.integrations.sourceControl = await initSourceControl(message.metadata.userId, message.metadata.sourceControl);

    const { namespace, pagination, repository, timePeriod } = message.content;

    if (!namespace) throw new Error("Invalid namespace id");

    const { mergeRequests } = await getMergeRequests(
      {
        externalRepositoryId: repository.externalId,
        namespaceName: namespace.name,
        repositoryName: repository.name,
        repositoryId: repository.id,
        page: pagination.page,
        perPage: pagination.perPage,
        timePeriod,
      },
      context,
    );

    await extractMergeRequestsEvent.publish({ mergeRequestIds: mergeRequests.map(mr => mr.id), namespaceId: namespace.id, repositoryId: repository.id }, {
      version: 1,
      caller: 'extract-merge-requests',
      sourceControl: message.metadata.sourceControl,
      userId: message.metadata.userId,
      timestamp: new Date().getTime(),
    });

  }
});

const { sender } = mergeRequestSenderHandler;

const client = createClient({
  url: Config.DATABASE_URL,
  authToken: Config.DATABASE_AUTH_TOKEN,
});

const db = drizzle(client);

const context: Context<
  GetMergeRequestsSourceControl,
  GetMergeRequestsEntities
> = {
  entities: {
    mergeRequests,
  },
  integrations: {
    sourceControl: null,
  },
  db,
};

const initSourceControl = async (userId: string, sourceControl: 'github' | 'gitlab') => {
  const accessToken = await getClerkUserToken(userId, `oauth_${sourceControl}`);
  if (sourceControl === 'github') return new GitHubSourceControl(accessToken);
  if (sourceControl === 'gitlab') return new GitlabSourceControl(accessToken);
  return null;
}

export const eventHandler = EventHandler(extractRepositoryEvent, async (evt) => {
  const repository = await db.select().from(repositories).where(eq(repositories.id, evt.properties.repositoryId)).get();
  const namespace = await db.select().from(namespaces).where(eq(namespaces.id, evt.properties.namespaceId)).get();

  if (!repository) throw new Error("invalid repo id");
  if (!namespace) throw new Error("Invalid namespace id");

  const sourceControl = evt.metadata.sourceControl;

  context.integrations.sourceControl = await initSourceControl(evt.metadata.userId, sourceControl)

  const startDate = new Date();
  startDate.setMonth(startDate.getMonth() - 6);
  startDate.setDate(startDate.getDate() - 14);

  const timePeriod = {
    from: startDate,
    to: new Date(),
  }

  const { mergeRequests, paginationInfo } = await getMergeRequests(
    {
      externalRepositoryId: repository.externalId,
      namespaceName: namespace.name,
      repositoryName: repository.name,
      repositoryId: repository.id,
      perPage: 10,
      timePeriod,
    }, context,
  );

  await extractMergeRequestsEvent.publish({ mergeRequestIds: mergeRequests.map(mr => mr.id), namespaceId: namespace.id, repositoryId: repository.id }, {
    version: 1,
    caller: 'extract-merge-requests',
    sourceControl,
    userId: evt.metadata.userId,
    timestamp: new Date().getTime(),
  });

  const arrayOfExtractMergeRequests = [];
  for (let i = 2; i <= paginationInfo.totalPages; i++) {
    arrayOfExtractMergeRequests.push({
      repository,
      namespace: namespace,
      pagination: {
        page: i,
        perPage: paginationInfo.perPage,
        totalPages: paginationInfo.totalPages
      },
      timePeriod,
    });
  }

  await sender.sendAll(arrayOfExtractMergeRequests, {
    version: 1,
    caller: 'extract-merge-requests',
    sourceControl,
    userId: evt.metadata.userId,
    timestamp: new Date().getTime(),
  });

})
