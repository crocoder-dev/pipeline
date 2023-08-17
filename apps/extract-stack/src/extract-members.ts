import { EventHandler } from "sst/node/event-bus";
import { extractRepositoryEvent } from "./events";
import { Clerk } from "@clerk/clerk-sdk-node";
import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { getMembers } from "@acme/extract-functions";
import type { Context, GetMembersEntities, GetMembersSourceControl } from "@acme/extract-functions";
import { members, namespaces, repositories, repositoriesToMembers } from "@acme/extract-schema";
import type { Namespace, Repository } from "@acme/extract-schema";
import { GitHubSourceControl, GitlabSourceControl } from "@acme/source-control";
import type { Pagination } from "@acme/source-control";
import { Config } from "sst/node/config";
import { extractMemberPageMessage } from "./messages";

import { QueueHandler } from "./create-message";
import { eq } from "drizzle-orm";

const clerkClient = Clerk({ secretKey: Config.CLERK_SECRET_KEY });
const client = createClient({ url: Config.DATABASE_URL, authToken: Config.DATABASE_AUTH_TOKEN });

const fetchSourceControlAccessToken = async (userId: string, forgeryIdProvider: 'oauth_github' | 'oauth_gitlab') => {
  const [userOauthAccessTokenPayload, ...rest] = await clerkClient.users.getUserOauthAccessToken(userId, forgeryIdProvider);
  if (!userOauthAccessTokenPayload) throw new Error("Failed to get token");
  if (rest.length !== 0) throw new Error("wtf ?");

  return userOauthAccessTokenPayload.token;
}

const initSourceControl = async (userId: string, sourceControl: 'github' | 'gitlab') => {
  const accessToken = await fetchSourceControlAccessToken(userId, `oauth_${sourceControl}`);
  if (sourceControl === 'github') return new GitHubSourceControl(accessToken);
  if (sourceControl === 'gitlab') return new GitlabSourceControl(accessToken);
  return null;
}

const db = drizzle(client);

const context: Context<GetMembersSourceControl, GetMembersEntities> = {
  entities: {
    members,
    repositoriesToMembers
  },
  integrations: {
    sourceControl: null,
  },
  db,
};

type ExtractMembersPageInput = {
  namespace: Namespace | null;
  repository: Repository;
  sourceControl: "github" | "gitlab";
  userId: string;
  paginationInfo?: Pagination;
}

const extractMembersPage = async ({ namespace, repository, sourceControl, userId, paginationInfo }: ExtractMembersPageInput) => {
  const page = paginationInfo?.page;
  const perPage = paginationInfo?.perPage;

  context.integrations.sourceControl = await initSourceControl(userId, sourceControl);

  const { paginationInfo: resultPaginationInfo } = await getMembers({
    externalRepositoryId: repository.externalId,
    namespaceName: namespace?.name || "",
    repositoryId: repository.id,
    repositoryName: repository.name,
    perPage: perPage,
    page: page
  }, context);

  return resultPaginationInfo;
};

export const eventHandler = EventHandler(extractRepositoryEvent, async (ev) => {
  if (!ev.properties.namespaceId) throw new Error("Missing namespaceId");

  const repository = await db.select().from(repositories).where(eq(repositories.id, ev.properties.repositoryId)).get();
  const namespace = await db.select().from(namespaces).where(eq(namespaces.id, ev.properties.namespaceId)).get();
  
  if (!repository) throw new Error("invalid repo id");
  if (!namespace) throw new Error("Invalid namespace id");

  const pagination = await extractMembersPage({
    namespace: namespace,
    repository: repository,
    sourceControl: ev.metadata.sourceControl,
    userId: ev.metadata.userId,
  });

  const arrayOfExtractMemberPageMessageContent: { repository: Repository, namespace: Namespace | null, pagination: Pagination }[] = [];
  for (let i = 2; i <= pagination.totalPages; i++) {
    arrayOfExtractMemberPageMessageContent.push({
      namespace: namespace,
      repository: repository,
      pagination: {
        page: i,
        perPage: pagination.perPage,
        totalPages: pagination.totalPages
      }
    })
  }

  if (arrayOfExtractMemberPageMessageContent.length === 0) return console.log("No more pages left, no need to enqueue");

  await extractMemberPageMessage.sendAll(arrayOfExtractMemberPageMessageContent, {
    version: 1,
    caller: 'extract-member',
    sourceControl: ev.metadata.sourceControl,
    userId: ev.metadata.userId,
    timestamp: new Date().getTime(),
  })

});

export const queueHandler = QueueHandler(extractMemberPageMessage, async (message) => {
  await extractMembersPage({
    namespace: message.content.namespace,
    paginationInfo: message.content.pagination,
    repository: message.content.repository,
    sourceControl: message.metadata.sourceControl,
    userId: message.metadata.userId
  });
});