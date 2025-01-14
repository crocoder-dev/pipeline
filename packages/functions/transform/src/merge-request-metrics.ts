import * as extract from '@dxta/extract-schema';
import * as transform from '@dxta/transform-schema';
import * as tenant from '@dxta/tenant-schema';
import { sql, eq, or, and, type ExtractTablesWithRelations } from "drizzle-orm";
import type { LibSQLDatabase } from 'drizzle-orm/libsql';
import { isCodeGen } from './is-codegen';
import { parseHunks } from './parse-hunks';
import { type SQLiteTransaction } from 'drizzle-orm/sqlite-core';
import { type ResultSet } from '@libsql/client/.';
import { isMemberKnownBot } from './known-bots';
import { getDateInfo } from '../../../schemas/transform/src/seed/dimensions';
import { compare } from './compare';
import { getUTCOffset } from './timezone-utils';


type BrandedDatabase<T> = LibSQLDatabase<Record<string, never>> & { __brand: T }
type DatabaseTransaction = SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>;

export type TransformDatabase = BrandedDatabase<'transform'>;
export type ExtractDatabase = BrandedDatabase<'extract'>;
export type TenantDatabase = BrandedDatabase<'tenant'>;

type TableMeta = {
  _createdAt: Date | null;
  _updatedAt: Date | null;
}

async function getHqTimezone(db: TenantDatabase): Promise<string> {
  const result = await db.select({ tzdata: tenant.tenantConfig.tzdata })
    .from(tenant.tenantConfig)
    .limit(1);

    const timezone = result?.[0]?.tzdata || 'UTC';
    return timezone;
}


function getUsersDatesMergeRequestMetricsId(db: TransformDatabase, transformMergeRequestId: number) {
  return db.select({
    usersJunkId: transform.mergeRequestMetrics.usersJunk,
    datesJunkId: transform.mergeRequestMetrics.datesJunk,
    mergeRequestMetricId: transform.mergeRequestMetrics.id,
  })
    .from(transform.mergeRequestMetrics)
    .where(
      eq(transform.mergeRequestMetrics.mergeRequest, transformMergeRequestId),
    );
}

function insertMergeMetrics(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, mergeMetrics: transform.NewMergeRequestMetric) {
  return tx.insert(transform.mergeRequestMetrics)
    .values(mergeMetrics);
}

function insertMergeRequestEvent(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, mergeRequestEvent: transform.NewMergeRequestEvent) {
  return tx.insert(transform.mergeRequestEvents)
    .values(mergeRequestEvent);
}

function deleteMergeRequestEvents(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, mergeRequestId: transform.MergeRequest["id"]) {
  return tx.delete(transform.mergeRequestEvents)
    .where(
      eq(transform.mergeRequestEvents.mergeRequest, mergeRequestId)
    );
}

function updateMergeMetrics(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, mergeMetrics: Omit<transform.MergeRequestMetric, keyof TableMeta>) {
  return tx.update(transform.mergeRequestMetrics)
    .set({
      ...mergeMetrics,
      _updatedAt: sql`(strftime('%s', 'now'))`,
    })
    .where(
      eq(transform.mergeRequestMetrics.id, mergeMetrics.id)
    );
}

function upsertRepository(db: TransformDatabase, repo: transform.NewRepository) {
  return db.insert(transform.repositories)
    .values(repo)
    .onConflictDoUpdate({
      target: [
        transform.repositories.externalId,
        transform.repositories.forgeType
      ],
      set: {
        name: repo.name,
        namespaceName: repo.namespaceName,
        defaultBranch: repo.defaultBranch,
        _updatedAt: sql`(strftime('%s', 'now'))`,
      },
    })
    .returning();
}

function upsertDeployment(db: TransformDatabase, deploy: transform.NewDeployment) {
  return db.insert(transform.deployments)
  .values(deploy)
  .onConflictDoUpdate({
    target: [
      transform.deployments.externalId,
      transform.deployments.repositoryId,
      transform.deployments.deploymentType
    ],
    set: {
      deploymentType: deploy.deploymentType,
      deployedAt: deploy.deployedAt,
      status: deploy.status,
      _updatedAt: sql`(strftime('%s', 'now'))`,
    }
  });
}

function upsertBranch(db: TransformDatabase, branch: transform.NewBranch) {
  return db.insert(transform.branches)
    .values(branch)
    .onConflictDoUpdate({
      target: [
        transform.branches.name
      ],
      set: {
        _updatedAt: sql`(strftime('%s', 'now'))`,
      }
    })
    .returning();
}

function upsertMergeRequest(db: TransformDatabase, mergeRequest: transform.NewMergeRequest) {
  return db.insert(transform.mergeRequests)
    .values(mergeRequest)
    .onConflictDoUpdate({
      target: [
        transform.mergeRequests.externalId,
        transform.mergeRequests.forgeType
      ],
      set: {
        title: mergeRequest.title,
        description: mergeRequest.description,
        webUrl: mergeRequest.webUrl,
        mergeCommitSha: mergeRequest.mergeCommitSha,
        _updatedAt: sql`(strftime('%s', 'now'))`,
      }
    })
    .returning();
}

function upsertForgeUser(db: DatabaseTransaction | TransformDatabase, forgeUser: transform.NewForgeUser) {
  return db.insert(transform.forgeUsers)
    .values(forgeUser)
    .onConflictDoUpdate({
      target: [
        transform.forgeUsers.externalId,
        transform.forgeUsers.forgeType,
      ],
      set: {
        name: forgeUser.name,
        avatarUrl: forgeUser.avatarUrl,
        profileUrl: forgeUser.profileUrl,
        _updatedAt: sql`(strftime('%s', 'now'))`,
      }
    })
}

function insertUserJunk(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, users: transform.NewMergeRequestUsersJunk) {
  return tx.insert(transform.mergeRequestUsersJunk)
    .values(users)
    .returning();
}

function updateUserJunk(db: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, users: Omit<transform.MergeRequestUsersJunk, keyof TableMeta>) {
  return db.update(transform.mergeRequestUsersJunk)
    .set({
      ...users,
      _updatedAt: sql`(strftime('%s', 'now'))`,
    })
    .where(eq(transform.mergeRequestUsersJunk.id, users.id))
}

function insertDateJunk(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, dates: transform.NewMergeRequestDatesJunk) {
  return tx.insert(transform.mergeRequestDatesJunk)
    .values(dates)
    .returning();
}

function updateDateJunk(tx: SQLiteTransaction<"async", ResultSet, Record<string, unknown>, ExtractTablesWithRelations<Record<string, unknown>>>, dates: Omit<transform.MergeRequestDatesJunk, keyof TableMeta>) {
  return tx.update(transform.mergeRequestDatesJunk)
    .set({
      ...dates,
      _updatedAt: sql`(strftime('%s', 'now'))`,
    })
    .where(eq(transform.mergeRequestDatesJunk.id, dates.id))
}

async function selectNullRows(db: TransformDatabase) {
  const { nullRows } = transform;

  const nullRowsData = await db.select({
    dateId: nullRows.dateId,
    userId: nullRows.userId,
    mergeRequestId: nullRows.mergeRequestId,
    repositoryId: nullRows.repositoryId,
  }).from(nullRows)
    .get();

  if (!nullRowsData) {
    throw new Error('No null rows found');
  }

  return nullRowsData;

}

type DMY = {
  year: number,
  month: number,
  day: number,
  week: string,
};

export function getDMY(date: Date | null, timeZone: string) {
  if (date === null) {
    return null;
  }
  const tzOffsetInMinutes = getUTCOffset(timeZone, date);
  const tzOffset = tzOffsetInMinutes * 60 * 1000;
  const tzDate = new Date(date.getTime() + tzOffset);
  return getDateInfo(new Date(Date.UTC(tzDate.getUTCFullYear(), tzDate.getUTCMonth(), tzDate.getUTCDate())))
}

function getDMYQuery(dmy: DMY | null) {
  if (dmy === null) {
    return undefined;
  }
  return and(
    eq(transform.dates.year, dmy.year),
    eq(transform.dates.month, dmy.month),
    eq(transform.dates.day, dmy.day),
    eq(transform.dates.week, dmy.week),
  );
}

function getDateIdOrNullDateId(dmy: DMY | null, datesData: {
  id: number;
  year: number;
  month: number;
  day: number;
  week: string;
}[], nullDateId: number) {
  if (dmy === null) {
    return {
      id: nullDateId,
    };
  }
  const date = datesData.find(({ year, month, day, week }) => year === dmy.year && month === dmy.month && day === dmy.day && week === dmy.week);
  if (!date) {
    console.error(`No date found for ${JSON.stringify(dmy)}`);
    return {
      id: nullDateId,
    };
  }
  return {
    id: date.id,
    day: date.day,
    month: date.month,
    year: date.year,
    week: date.week,
  };
}

async function upsertForgeUsers(db: TransformDatabase, members: MemberData[]) {

  const uniqueMembers = members.reduce((acc, member) => {
    if (!acc.some(m => m.externalId === member.externalId)) {
      acc.push(member);
    }
    return acc;
  }, [] as MemberData[]);


  const newForgeUsers = uniqueMembers.map((member) => ({
    externalId: member.externalId,
    forgeType: member.forgeType,
    name: member.name || member.username,
    avatarUrl: member.avatarUrl,
    profileUrl: member.profileUrl,
    bot: isMemberKnownBot(member.forgeType, member),
  } satisfies transform.NewForgeUser));

  const forgeUsers = await db.transaction(async tx => {
    return await Promise.all(newForgeUsers.map(x => upsertForgeUser(tx, x).returning().get()));
  });

  return forgeUsers satisfies transform.ForgeUser[];
}


async function selectDates(db: TransformDatabase, timestamps: Set<number>, timezone: string) {

  const query = [];
  for (const timestamp of timestamps) {
    query.push(getDMYQuery(getDMY(new Date(timestamp), timezone)));
  }

  const datesData = await db.select({
    id: transform.dates.id,
    year: transform.dates.year,
    month: transform.dates.month,
    day: transform.dates.day,
    week: transform.dates.week,
  }).from(transform.dates)
    .where(
      or(
        ...query
      )
    )
    .all();

  return datesData;
}

type MapUsersToJunksArgs = {
  author: transform.ForgeUser['id'] | null | undefined,
  mergedBy: transform.ForgeUser['id'] | null | undefined,
  approvers: transform.ForgeUser['id'][],
  committers: transform.ForgeUser['id'][],
  reviewers: transform.ForgeUser['id'][]
}

function getCommitter(gitIdentity: extract.CommittedEvent & { committerId: number | null }, members: MemberData[]): MemberData[] {

  function splitMembersByEmail(member: MemberData, email: string) {
    if (member.email === email) {
      return [member];
    }
    return [member, {
      externalId: member.externalId,
      forgeType: member.forgeType,
      name: member.name,
      username: member.username,
      email: email,
      avatarUrl: member.avatarUrl,
      profileUrl: member.profileUrl,
    }] satisfies MemberData[];
  }

  if (gitIdentity.committerId !== null) {
    const member = members.find((m) => m.externalId === gitIdentity.committerId);
    if (member) {
      return [member];
    }
  }

  let member = members.find((m) => m.email !== null && m.email?.toLowerCase() === gitIdentity.committerEmail?.toLowerCase());
  if (member) {
    return [member];
  }

  member = members.find((m) => m.name !== null && m.name?.toLowerCase() === gitIdentity.committerName.toLowerCase());
  if (member) {
    return splitMembersByEmail(member, gitIdentity.committerEmail);
  }

  member = members.find((m) => m.username.toLowerCase() === gitIdentity.committerName.toLowerCase());
  if (member) {
    return splitMembersByEmail(member, gitIdentity.committerEmail);
  }

  member = members.find((m) => !!m.name && compare(m.name, gitIdentity.committerName));
  if (member) {
    return splitMembersByEmail(member, gitIdentity.committerEmail);
  }

  member = members.find((m) => !!m.email && compare(m.email, gitIdentity.committerEmail));
  if (member) {
    return splitMembersByEmail(member, gitIdentity.committerEmail);
  }

  member = members.find((m) => m.username !== null && compare(m.username, gitIdentity.committerName));
  if (member) {
    return splitMembersByEmail(member, gitIdentity.committerEmail);
  }

  return [];
}


function mapUsersToJunk({ author, mergedBy, approvers, committers, reviewers }: MapUsersToJunksArgs, nullForgeUserId: number) {
  return {
    author: author || nullForgeUserId,
    mergedBy: mergedBy || nullForgeUserId,
    approver1: approvers[0] || nullForgeUserId,
    approver2: approvers[1] || nullForgeUserId,
    approver3: approvers[2] || nullForgeUserId,
    approver4: approvers[3] || nullForgeUserId,
    approver5: approvers[4] || nullForgeUserId,
    approver6: approvers[5] || nullForgeUserId,
    approver7: approvers[6] || nullForgeUserId,
    approver8: approvers[7] || nullForgeUserId,
    approver9: approvers[8] || nullForgeUserId,
    approver10: approvers[9] || nullForgeUserId,
    committer1: committers[0] || nullForgeUserId,
    committer2: committers[1] || nullForgeUserId,
    committer3: committers[2] || nullForgeUserId,
    committer4: committers[3] || nullForgeUserId,
    committer5: committers[4] || nullForgeUserId,
    committer6: committers[5] || nullForgeUserId,
    committer7: committers[6] || nullForgeUserId,
    committer8: committers[7] || nullForgeUserId,
    committer9: committers[8] || nullForgeUserId,
    committer10: committers[9] || nullForgeUserId,
    reviewer1: reviewers[0] || nullForgeUserId,
    reviewer2: reviewers[1] || nullForgeUserId,
    reviewer3: reviewers[2] || nullForgeUserId,
    reviewer4: reviewers[3] || nullForgeUserId,
    reviewer5: reviewers[4] || nullForgeUserId,
    reviewer6: reviewers[5] || nullForgeUserId,
    reviewer7: reviewers[6] || nullForgeUserId,
    reviewer8: reviewers[7] || nullForgeUserId,
    reviewer9: reviewers[8] || nullForgeUserId,
    reviewer10: reviewers[9] || nullForgeUserId,
  } satisfies transform.NewMergeRequestUsersJunk;
}

function calculateMrSize(mergeRequestId: number, diffs: { stringifiedHunks: string, newPath: string }[]): {
  mrSize: number,
  codeAddition: number,
  codeDeletion: number,
} {
  if (diffs.length === 0) {
    console.log("Warn: 0 Diffs found for merge request with id: ", mergeRequestId);
    return {
      mrSize: 0,
      codeAddition: 0,
      codeDeletion: 0,
    }
  }

  let mrSize = 0;
  let codeAddition = 0;
  let codeDeletion = 0;

  for (const diff of diffs) {
    const isVeryLargeDiff = diff.newPath === 'urn:dxta-pipeline:very-large-diff' && diff.stringifiedHunks === '';
    const codeGenResult = isCodeGen(diff.newPath);

    // Note: Reasoning for not increasing additions/deletions is because we can't know this information. Might affect addition/deletion statistics
    if (isVeryLargeDiff) {
      mrSize += 29999984;
      continue;
    }

    if (codeGenResult === true) {
      continue;
    }

    const result = parseHunks(diff.stringifiedHunks);

    mrSize += result
      .map(({ additions, deletions }) => additions + deletions)
      .reduce((a, b) => a + b, 0);

    const { add, del } = result
      .map(({ additions, deletions }) => ({ add: additions, del: deletions }))
      .reduce((a, b) => ({ add: a.add + b.add, del: a.del + b.del }), { add: 0, del: 0 });

    codeAddition += add;
    codeDeletion += del;
  }

  return { mrSize, codeAddition, codeDeletion };
}

function calculateDuration(start: Date | null, end: Date | null) {
  if (!start || !end) {
    return 0;
  }
  return end.getTime() - start.getTime();
}

type MemberData = {
  forgeType: extract.Member['forgeType'];
  externalId: extract.Member['externalId'];
  name: extract.Member['name'];
  username: extract.Member['username'];
  email: extract.Member['email'];
  avatarUrl: extract.Member['avatarUrl'];
  profileUrl: extract.Member['profileUrl'];
}

type MergeRequestData = {
  openedAt: extract.MergeRequest['createdAt'],
  updatedAt: extract.MergeRequest['updatedAt'],
  mergedAt: extract.MergeRequest['mergedAt'],
  closedAt: extract.MergeRequest['closedAt'],
  externalId: extract.MergeRequest['externalId'],
  authorExternalId: extract.MergeRequest['authorExternalId']
}

export type TimelineEventData = {
  type: extract.TimelineEvents['type'];
  timestamp: extract.TimelineEvents['timestamp'];
  actorId: extract.TimelineEvents['actorId'];
  actorName: extract.TimelineEvents['actorName']; // is commitAuthorName for commit events
  actorEmail: extract.TimelineEvents['actorEmail'];
  htmlUrl: extract.TimelineEvents['htmlUrl'];
  data: extract.TimelineEvents['data'];
}

export type MergeRequestNoteData = {
  type: 'note';
  timestamp: extract.MergeRequestNote['createdAt'];
  authorExternalId: extract.MergeRequestNote['authorExternalId'];
}

async function selectExtractData(db: ExtractDatabase, extractMergeRequestId: number, extractDeploymentId: number | null) {
  const { mergeRequests, mergeRequestDiffs, mergeRequestNotes, timelineEvents, repositories, namespaces, mergeRequestCommits, repositoryShas, deployments } = extract;
  const mergeRequestData = await db.select({
    mergeRequest: {
      openedAt: mergeRequests.createdAt,
      mergedAt: mergeRequests.mergedAt,
      closedAt: mergeRequests.closedAt,
      externalId: mergeRequests.externalId,
      canonId: mergeRequests.canonId,
      authorExternalId: mergeRequests.authorExternalId,
      updatedAt: mergeRequests.updatedAt,
      repositoryId: mergeRequests.repositoryId,
      title: mergeRequests.title,
      description: mergeRequests.description,
      webUrl: mergeRequests.webUrl,
      sourceBranch: mergeRequests.sourceBranch,
      targetBranch: mergeRequests.targetBranch,
      mergeCommitSha: repositoryShas.sha,
    }
  }).from(mergeRequests)
    .where(eq(mergeRequests.id, extractMergeRequestId))
    .leftJoin(repositoryShas, eq(repositoryShas.id, mergeRequests.mergeCommitShaId))
    .get();

  const repositoryData = await db.select({
    repository: {
      externalId: repositories.externalId,
      name: repositories.name,
      forgeType: repositories.forgeType,
      namespaceName: namespaces.name,
      defaultBranchName: repositories.defaultBranch,
    }
  }).from(repositories)
    .where(eq(repositories.id, mergeRequestData?.mergeRequest.repositoryId || 0))
    .innerJoin(namespaces, eq(namespaces.id, repositories.namespaceId))
    .get();

  const membersData = await db.select({
    forgeType: extract.members.forgeType,
    externalId: extract.members.externalId,
    name: extract.members.name,
    username: extract.members.username,
    email: extract.members.email,
    profileUrl: extract.members.profileUrl,
    avatarUrl: extract.members.avatarUrl,
  })
    .from(extract.members)
    .where(eq(extract.members.forgeType, repositoryData?.repository.forgeType || "github")) // idk man
    .all() satisfies MemberData[];

  const mergerRequestDiffsData = await db.select({
    stringifiedHunks: mergeRequestDiffs.diff,
    newPath: mergeRequestDiffs.newPath,
  })
    .from(mergeRequestDiffs)
    .where(eq(mergeRequestDiffs.mergeRequestId, extractMergeRequestId))
    .all();

  const mergeRequestNotesData = await db.select({
    timestamp: mergeRequestNotes.createdAt,
    authorExternalId: mergeRequestNotes.authorExternalId,
    htmlUrl: mergeRequestNotes.htmlUrl,
  })
    .from(mergeRequestNotes)
    .where(eq(mergeRequestNotes.mergeRequestId, extractMergeRequestId))
    .all() satisfies Omit<MergeRequestNoteData, 'type'>[];

  const timelineEventsData = await db.select({
    type: timelineEvents.type,
    timestamp: timelineEvents.timestamp,
    actorId: timelineEvents.actorId,
    actorName: timelineEvents.actorName,
    actorEmail: timelineEvents.actorEmail,
    htmlUrl: timelineEvents.htmlUrl,
    data: timelineEvents.data
  })
    .from(timelineEvents)
    .where(eq(timelineEvents.mergeRequestId, extractMergeRequestId))
    .all() satisfies TimelineEventData[];

  const commitsData = await db.select({
    authoredDate: mergeRequestCommits.authoredDate,
    authorExternalId: mergeRequestCommits.authorExternalId,
    authorName: mergeRequestCommits.authorName,
    authorEmail: mergeRequestCommits.authorEmail,
    committedDate: mergeRequestCommits.committedDate,
    committerExternalId: mergeRequestCommits.committerExternalId,
    committerName: mergeRequestCommits.committerName,
    committerEmail: mergeRequestCommits.committerEmail,
    createdAt: mergeRequestCommits.createdAt,
    htmlUrl: mergeRequestCommits.htmlUrl,
  })
    .from(mergeRequestCommits)
    .where(eq(mergeRequestCommits.mergeRequestId, extractMergeRequestId))
    .all();

  let deploymentData: {
    deployedAt: extract.Deployment['deployedAt'];
    status: extract.Deployment['status'];
    type: extract.Deployment['deploymentType'];
    externalId: extract.Deployment['externalId'];
    // commitSha: string | null;
    environment: extract.Deployment['environment'];
    gitBranch: extract.Deployment['gitBranch'];
  } | undefined;
  if (extractDeploymentId) {
    deploymentData = await db.select({
      deployedAt: deployments.deployedAt,
      status: deployments.status,
      type: deployments.deploymentType,
      externalId: deployments.externalId,
      environment: deployments.environment,
      gitBranch: deployments.gitBranch,
      // commitSha: repositoryShas.sha,
    })
      .from(deployments)
      .where(eq(deployments.id, extractDeploymentId))
      // .leftJoin(repositoryShas, eq(repositoryShas.id, deployments.repositoryShaId))
      .get();
  }

  return {
    diffs: mergerRequestDiffsData,
    ...mergeRequestData || { mergeRequest: null },
    notes: mergeRequestNotesData.map(note => ({ ...note, type: 'note' as const })),
    timelineEvents: timelineEventsData,
    ...repositoryData || { repository: null },
    members: membersData,
    commits: commitsData,
    deployment: deploymentData
  };
}

export type RunContext = {
  extractDatabase: ExtractDatabase;
  transformDatabase: TransformDatabase;
  tenantDatabase: TenantDatabase;
};


export type TimelineMapKey = {
  type: extract.TimelineEvents['type'] | 'note' | 'opened',
  timestamp: Date,
}

function setupTimeline(timelineEvents: TimelineEventData[], notes: MergeRequestNoteData[]) {
  const timeline = new Map<TimelineMapKey,
    TimelineEventData | MergeRequestNoteData
  >();

  for (const timelineEvent of timelineEvents) {
    timeline.set({
      type: timelineEvent.type,
      timestamp: timelineEvent.timestamp,
    }, timelineEvent);
  }

  for (const note of notes) {
    timeline.set({
      type: 'note',
      timestamp: note.timestamp,
    }, note);
  }

  return timeline;

}

type calcTimelineArgs = {
  authorExternalId: extract.MergeRequest['authorExternalId'],
  createdAt: extract.MergeRequest['createdAt'] | null,
}


function getStartedCodingAt(timelineMapKeys: TimelineMapKey[], createdAt: Date | null) {
  const firstCommitEvent = timelineMapKeys.find(key => key.type === 'committed');

  if (!firstCommitEvent) return null;

  if (!createdAt) {
    return firstCommitEvent.timestamp;
  }

  return firstCommitEvent.timestamp < createdAt ? firstCommitEvent.timestamp : createdAt;
}

function getMergedAt(timelineMapKeys: TimelineMapKey[]) {
  const firstMergedEvent = timelineMapKeys.find(key => key.type === 'merged');

  if (firstMergedEvent) {
    return firstMergedEvent.timestamp;
  }

  return null;
}

type OpenedEventData = {
  actorId: extract.MergeRequest['authorExternalId'],
  type: 'opened',
}

export function calculateTimeline(timelineMapKeys: TimelineMapKey[], timelineMap: Map<TimelineMapKey, MergeRequestNoteData | TimelineEventData | OpenedEventData>, { authorExternalId, createdAt }: calcTimelineArgs) {

  const sortedTimelineMapKeys = [...timelineMapKeys]
  sortedTimelineMapKeys.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

  const committedEvents = sortedTimelineMapKeys.filter(key => key.type === 'committed');
  const lastCommitEvent = committedEvents[committedEvents.length - 1] || null;

  const startedCodingAt = getStartedCodingAt(committedEvents, createdAt);
  const mergedAt = getMergedAt(sortedTimelineMapKeys);

  const readyForReviewEvents = sortedTimelineMapKeys.filter(key => key.type === 'ready_for_review' || key.type === 'review_requested');
  const lastReadyForReviewEvent = readyForReviewEvents[readyForReviewEvents.length - 1] || null;

  const startedPickupAt = (() => {
    if (lastCommitEvent === null && lastReadyForReviewEvent === null) {
      return null;
    }
    if (lastReadyForReviewEvent === null && lastCommitEvent) {
      // problematic code: everything below is problematic
      const reviewedEventsBeforeLastCommitEvent = sortedTimelineMapKeys.filter(key => key.type === 'reviewed' && key.timestamp < lastCommitEvent.timestamp);
      const firstReviewedEventBeforeLastCommitEvent = reviewedEventsBeforeLastCommitEvent[0];
      if (firstReviewedEventBeforeLastCommitEvent) {
        return [...committedEvents].reverse().find(event => event.timestamp < firstReviewedEventBeforeLastCommitEvent.timestamp)?.timestamp || null;
      }

      return lastCommitEvent.timestamp;
    }
    if (lastReadyForReviewEvent && lastCommitEvent) {
      // problematic code: there could be a commit between last commit and lastReadyForReviewEvent
      const reviewedEventsAfterLastReadyForReviewEvent = sortedTimelineMapKeys.filter(
        key =>
          key.type === 'reviewed'
          && key.timestamp > lastReadyForReviewEvent.timestamp
          && key.timestamp < lastCommitEvent.timestamp
      );
      const firstReviewedEventAfterLastReadyForReviewEvent = reviewedEventsAfterLastReadyForReviewEvent[0]

      if (firstReviewedEventAfterLastReadyForReviewEvent) {
        const temp = [...committedEvents].reverse().find(
          event => event.timestamp > lastReadyForReviewEvent.timestamp
            && event.timestamp < firstReviewedEventAfterLastReadyForReviewEvent.timestamp

        )?.timestamp || null;

        if (temp) {
          return temp;
        }
        return lastReadyForReviewEvent.timestamp;
      }
      return lastReadyForReviewEvent.timestamp > lastCommitEvent.timestamp ? lastReadyForReviewEvent.timestamp : lastCommitEvent.timestamp;
    }

    return null;
  })();

  let firstReviewedEvent = null;
  let reviewed = false;
  let reviewDepth = 0;

  const noteEvents = timelineMapKeys.filter(key => key.type === 'note');
  for (const noteEvent of noteEvents) {
    const eventData = timelineMap.get(noteEvent) as MergeRequestNoteData | undefined;
    if (!eventData) {
      console.error('note event data not found', noteEvent);
      continue;
    }

    // Edge-case: what if note is before first review event ?
    const afterStartedPickupAt = startedPickupAt ? noteEvent.timestamp > startedPickupAt : true;
    const beforeMergedEvent = mergedAt ? noteEvent.timestamp < mergedAt : true;
    const isAuthorReviewer = eventData.authorExternalId === authorExternalId;
    if (afterStartedPickupAt && beforeMergedEvent && !isAuthorReviewer) {
      reviewDepth++;
    }
  }

  const reviewedEvents = timelineMapKeys.filter(key => key.type === 'reviewed' && key.timestamp < (mergedAt || new Date()));
  for (const reviewedEvent of reviewedEvents) {
    const eventData = timelineMap.get(reviewedEvent);
    if (!eventData) {
      console.error('reviewed event data not found', reviewedEvent);
      continue;
    }
    const res = extract.ReviewedEventSchema.safeParse((eventData as TimelineEventData).data);
    if (!res.success) {
      console.error(res.error);
      continue;
    }
    const isValidState = res.data.state === 'approved' || res.data.state === 'changes_requested' || res.data.state === 'commented';
    const afterStartedPickupAt = startedPickupAt ? reviewedEvent.timestamp > startedPickupAt : true;
    const beforeFirstReviewedEvent = firstReviewedEvent ? reviewedEvent.timestamp < firstReviewedEvent.timestamp : true;
    const beforeMergedEvent = mergedAt ? reviewedEvent.timestamp < mergedAt : true;
    const isAuthorReviewer = (eventData as TimelineEventData).actorId === authorExternalId;

    if (isValidState && afterStartedPickupAt && beforeMergedEvent && !isAuthorReviewer) {
      reviewed = true;
      reviewDepth++;
    }

    if (isValidState && afterStartedPickupAt && beforeFirstReviewedEvent && !isAuthorReviewer) {
      reviewed = true;
      firstReviewedEvent = reviewedEvent;
    }
  }

  let prevActorId: number | null = null;
  let handover = 0;
  let isClosed = false;
  const handoverSortedTimelineMapKeys = [...sortedTimelineMapKeys];
  if (createdAt !== null) {
    const index = handoverSortedTimelineMapKeys.findIndex(key => key.timestamp >= createdAt);
    const openedEventKey = { timestamp: createdAt, type: 'opened' } as const;
    if (index === -1) {
      handoverSortedTimelineMapKeys.push(openedEventKey);
    } else {
      handoverSortedTimelineMapKeys.splice(index, 0, openedEventKey);
    }
    timelineMap.set(openedEventKey, { type: 'opened', actorId: authorExternalId });
  }
  handoverSortedTimelineMapKeys.forEach(key => {
    const value = timelineMap.get(key)
    if (value && value.type === 'note') {
      const authorExternalId = value.authorExternalId;
      if (prevActorId !== null && authorExternalId !== prevActorId) {
        handover++;
      }
      prevActorId = authorExternalId;
      return;
    }

    if (value && value.type == "closed") {
      isClosed = true;
    }

    if (value && value.type === "committed" && !isClosed) {
      const actorId = (value.data as { committerId: number | null }).committerId;
      if (prevActorId !== null && actorId !== prevActorId) {
        handover++;
      }
      prevActorId = actorId;
      return;
    }

    if (value && value.type === "reviewed" && !isClosed) {
      const actorId = (value as unknown as TimelineEventData).actorId;
      const isStatePending = (value.data as extract.ReviewedEvent).state === 'pending';

      if (prevActorId !== null && actorId !== prevActorId && !isStatePending) {
        handover++;
      }
      if (!isStatePending) {
        prevActorId = actorId;
      }
      return;
    }

    if (value && !isClosed) {
      const actorId = (value as unknown as TimelineEventData).actorId;
      if (prevActorId !== null && actorId !== prevActorId) {
        handover++;
      }
      prevActorId = actorId;
      return;
    }
  });




  return {
    startedCodingAt,
    startedPickupAt,
    startedReviewAt: firstReviewedEvent ? firstReviewedEvent.timestamp : null,
    mergedAt,
    reviewed,
    reviewDepth,
    handover,
  };
}

function runTimeline(mergeRequestData: MergeRequestData, timelineEvents: TimelineEventData[], notes: MergeRequestNoteData[]) {
  const timelineMap = setupTimeline(timelineEvents, notes);
  const timelineMapKeys = [...timelineMap.keys()];

  const { startedCodingAt, startedReviewAt, startedPickupAt, reviewed, reviewDepth, handover } = calculateTimeline(
    timelineMapKeys,
    timelineMap,
    {
      createdAt: mergeRequestData.openedAt,
      authorExternalId: mergeRequestData.authorExternalId,
    });

  // TODO: can this be optimized with the map ?
  const approved = timelineEvents.find(ev => ev.type === 'reviewed' && (ev.data as extract.ReviewedEvent).state === 'approved') !== undefined;


  const codingDuration = calculateDuration(startedCodingAt, startedPickupAt);
  const pickupDuration = calculateDuration(startedPickupAt, startedReviewAt);
  const reviewDuration = calculateDuration(startedReviewAt, mergeRequestData.closedAt);
  const timeToMerge = calculateDuration(startedCodingAt, mergeRequestData.mergedAt);

  return {
    startedCodingAt,
    startedReviewAt,
    startedPickupAt,
    reviewed,
    reviewDepth,
    approved,
    handover,
    codingDuration,
    pickupDuration,
    reviewDuration,
    timeToMerge,
  }
}

function getMergeRequestTimestamps({
  mergeRequest,
  timelineEvents,
  notes
}: {
  mergeRequest: MergeRequestData,
  timelineEvents: TimelineEventData[],
  notes: MergeRequestNoteData[],
}) {
  const timestamps = new Set<number>();

  timestamps.add(mergeRequest.openedAt.getTime());
  if (mergeRequest.closedAt) timestamps.add(mergeRequest.closedAt.getTime());
  if (mergeRequest.mergedAt) timestamps.add(mergeRequest.mergedAt.getTime());
  if (mergeRequest.updatedAt) timestamps.add(mergeRequest.updatedAt.getTime());

  for (const timelineEvent of timelineEvents) {

    // Required for transform.MergeRequest['commitedAt']
    // new Date() required bcuz JSON.parse(done by drizzle) doesn't coerce ISO8601 strings
    if (timelineEvent.type === 'committed') timestamps.add(new Date((timelineEvent.data as extract.CommittedEvent).committedDate).getTime());

    timestamps.add(timelineEvent.timestamp.getTime());
  }

  for (const note of notes) {
    timestamps.add(note.timestamp.getTime());
  }

  // TODO: map to Dates ?
  return timestamps;
}

function getMergeRequestMembers({
  members,
  mergeRequest,
  timelineEvents,
  notes,
}: {
  members: MemberData[],
  mergeRequest: MergeRequestData,
  timelineEvents: TimelineEventData[],
  notes: MergeRequestNoteData[],
}) {

  const membersArray = [];

  if (mergeRequest.authorExternalId !== null) {
    const author = members.find(m => m.externalId === mergeRequest.authorExternalId);
    if (author) membersArray.push(author);
  }

  for (const timelineEvent of timelineEvents) {
    const actor = members.find(m => m.externalId === timelineEvent.actorId);
    if (actor) {
      membersArray.push(actor);
    } else if (timelineEvent.actorEmail) {
      const actor = members.find(m => m.email === timelineEvent.actorEmail);
      if (actor) {
        membersArray.push(actor);
      }
    }

    if (timelineEvent.type === 'committed') {
      if (timelineEvent.actorEmail) {
        const author = {
          committedDate: timelineEvent.timestamp,
          committerName: timelineEvent.actorName,
          committerEmail: timelineEvent.actorEmail,
          committerId: timelineEvent.actorId,
        } satisfies (extract.CommittedEvent & { committerId: number | null });
        membersArray.push(...getCommitter(author, members));
      }
      membersArray.push(...getCommitter(timelineEvent.data as extract.CommittedEvent & { committerId: number | null }, members));
    }
  }

  for (const note of notes) {
    const author = members.find(m => m.externalId === note.authorExternalId);
    if (author) membersArray.push(author);
  }

  return membersArray;
}

export async function run(extractMergeRequestId: number, extractDeploymentId: number | null, ctx: RunContext) {
  const extractData = await selectExtractData(ctx.extractDatabase, extractMergeRequestId, extractDeploymentId);

  if (!extractData) {
    console.error(`No extract data found for merge request with id ${extractMergeRequestId}`);
    return null;
  }

  if (extractData.mergeRequest === null) {
    throw new Error(`No merge request found for id ${extractMergeRequestId}`);
  }

  if (extractData.repository === null) {
    throw new Error(`No repository found for id ${extractData.mergeRequest.repositoryId}`);
  }

  const {
    dateId: nullDateId,
    userId: nullUserId,
    mergeRequestId: _nullMergeRequestId,
    repositoryId: _nullRepositoryId
  } = await selectNullRows(ctx.transformDatabase);

  const { id: repositoryDefaultBranchId } = await upsertBranch(ctx.transformDatabase, {
    name: extractData.repository.defaultBranchName || "", // TODO(transform-schema-restart): is default branch nullable ? (check github/gitlab)
  })
    .get();

  const { id: transformRepositoryId } = await upsertRepository(ctx.transformDatabase, {
    externalId: extractData.repository.externalId,
    forgeType: extractData.repository.forgeType,
    name: extractData.repository.name,
    namespaceName: extractData.repository.namespaceName,
    defaultBranch: repositoryDefaultBranchId,
  })
    .get();

  const { id: transformSourceBranchId } = await upsertBranch(ctx.transformDatabase, {
    name: extractData.mergeRequest.sourceBranch || "",
  })
    .get();

  const { id: transformTargetBranchId } = await upsertBranch(ctx.transformDatabase, {
    name: extractData.mergeRequest.targetBranch || "",
  })
    .get();

  const { id: transformMergeRequestId } = await upsertMergeRequest(ctx.transformDatabase, {
    externalId: extractData.mergeRequest.externalId,
    canonId: extractData.mergeRequest.canonId,
    forgeType: extractData.repository.forgeType,
    title: extractData.mergeRequest.title,
    mergeCommitSha: extractData.mergeRequest.mergedAt ? extractData.mergeRequest.mergeCommitSha : null,
    description: extractData.mergeRequest.description,
    webUrl: extractData.mergeRequest.webUrl,
    sourceBranch: transformSourceBranchId,
    targetBranch: transformTargetBranchId,
  })
    .get();

  /**** Prepare deployment start ****/
  const deployed = !!extractData.deployment;
  const deployedAt = extractData.deployment && extractData.deployment.status === 'success' && extractData.deployment.deployedAt ? extractData.deployment.deployedAt : null;
  if (deployed && !extractData.deployment?.deployedAt) console.error(new Error(`missing deployedAt for deployment ${extractDeploymentId}, mr: ${extractMergeRequestId}`));
  if (deployed && extractData.deployment?.status !== 'success') console.error(new Error(`Invalid status for deployment ${extractDeploymentId}, mr: ${extractMergeRequestId}. Expected 'success' but got ${extractData.deployment?.status}`));

  /**** Prepare deployment end ****/

  /**** Prepare timeline from commits ****/
  const timelineEventsWithCorrectedCommits = extractData.timelineEvents.filter(ev =>
    ev.type !== 'committed'
  );

  extractData.commits.forEach(commit => {
    timelineEventsWithCorrectedCommits.push({
      type: 'committed',
      timestamp: commit.authoredDate,
      actorId: commit.authorExternalId,
      actorName: commit.authorName,
      actorEmail: commit.authorEmail,
      htmlUrl: commit.htmlUrl,
      data: {
        committerId: commit.committerExternalId,
        committerName: commit.committerName,
        committerEmail: commit.committerEmail,
        committedDate: commit.committedDate,
      }
    });
  });
  /**** Prepare end ****/

  /**** Transform forge users and filter bot events ****/
  const mergeRequestMembers = getMergeRequestMembers({
    members: extractData.members,
    mergeRequest: extractData.mergeRequest,
    timelineEvents: timelineEventsWithCorrectedCommits,
    notes: extractData.notes,
  });
  const forgeUsers = await upsertForgeUsers(ctx.transformDatabase, mergeRequestMembers);
  const isForgeUserBotByIdSearch = (memberExternalId: number | null) => {
    if (memberExternalId === null) return false; // if no id, cant tell if its a bot
    return forgeUsers.filter(m => m.externalId === memberExternalId).find(m => m.bot === true) !== undefined
  }

  const timelineEventsWithoutBots = timelineEventsWithCorrectedCommits.filter(event => {
    if (isForgeUserBotByIdSearch(event.actorId)) return false;
    if (event.type === 'committed') {
      if (isForgeUserBotByIdSearch((event.data as { committerId: number | null }).committerId)) return false;
    }
    return true;
  })
  /**** Transform forge users and filter bot events end****/


  /**** MergeRequestMetrics ****/

  // Get users and dates junk if they exist
  const userAndDatesJunkIdsResult = await getUsersDatesMergeRequestMetricsId(ctx.transformDatabase, transformMergeRequestId).get();
  const usersJunkId = userAndDatesJunkIdsResult?.usersJunkId || null;
  const datesJunkId = userAndDatesJunkIdsResult?.datesJunkId || null;
  const mergeRequestMetricsId = userAndDatesJunkIdsResult?.mergeRequestMetricId || null;


  // Calculate the size for MergeRequestMetrics
  const {
    mrSize,
    codeAddition,
    codeDeletion,
  } = calculateMrSize(extractMergeRequestId, extractData.diffs.filter(Boolean));


  // Caluculate the rest of the metrics for MergeRequestMetrics
  const timeline = runTimeline(extractData.mergeRequest, timelineEventsWithoutBots, extractData.notes);


  // Calculate merged and closed for MergeRequestMetrics
  const merged = extractData.mergeRequest.mergedAt !== null;
  const closed = extractData.mergeRequest.closedAt !== null;

  /**** MergeRequestMetrics End ****/

  /**** MergeRequestDatesJunk ****/
  const mergeRequestTimestamps = getMergeRequestTimestamps({
    mergeRequest: extractData.mergeRequest,
    timelineEvents: timelineEventsWithCorrectedCommits,
    notes: extractData.notes,
  });
  if (deployedAt) mergeRequestTimestamps.add(deployedAt.getTime());

  const timezone = await getHqTimezone(ctx.tenantDatabase)
  
  const mergeRequestDates = await selectDates(ctx.transformDatabase, mergeRequestTimestamps, timezone);
  
  const timestampDateMap = new Map<number, transform.TransformDate['id']>();
  mergeRequestTimestamps.forEach(ts => {
    timestampDateMap.set(ts, getDateIdOrNullDateId(getDMY(new Date(ts), timezone), mergeRequestDates, nullDateId).id);
  });  

  const mergedAtId = timestampDateMap.get(extractData.mergeRequest.mergedAt?.getTime() || 0) || nullDateId;
  const closedAtId = timestampDateMap.get(extractData.mergeRequest.closedAt?.getTime() || 0) || nullDateId;
  const openedAtId = timestampDateMap.get(extractData.mergeRequest.openedAt.getTime() || 0) || nullDateId;
  const startedCodingAtId = timestampDateMap.get(timeline.startedCodingAt?.getTime() || 0) || nullDateId;
  const lastUpdatedAtId = timestampDateMap.get(extractData.mergeRequest.updatedAt?.getTime() || 0) || nullDateId;
  const startedPickupAtId = timestampDateMap.get(timeline.startedPickupAt?.getTime() || 0) || nullDateId;
  const startedReviewAtId = timestampDateMap.get(timeline.startedReviewAt?.getTime() || 0) || nullDateId;
  /**** MergeRequestDatesJunk end ****/


  /**** MergeRequestUsersJunk ****/
  const memberEmailForgeUserIdMap = new Map<extract.Member['email'], transform.ForgeUser['id']>();
  const memberExternalIdForgeUserIdMap = new Map<extract.Member['externalId'], transform.ForgeUser['id']>();

  forgeUsers.forEach(forgeUser => {
    const members = mergeRequestMembers.filter(m => m.externalId === forgeUser.externalId);
    members.forEach(member => {
      if (member && member.email) {
        memberEmailForgeUserIdMap.set(member.email, forgeUser.id);
      }
      memberExternalIdForgeUserIdMap.set(forgeUser.externalId, forgeUser.id);
    });
  });

  const author = extractData.mergeRequest.authorExternalId
    ? memberExternalIdForgeUserIdMap.get(extractData.mergeRequest.authorExternalId) || nullUserId
    : nullUserId;
  const committers: number[] = [];
  const reviewers: number[] = [];
  const approvers: number[] = [];
  let mergedBy: number = nullUserId;

  const timelineEventForgeUsersIdsMap = new Map<TimelineEventData, { actorId: transform.ForgeUser['id'], committerId: transform.ForgeUser['id'] }>();

  for (const timelineEvent of timelineEventsWithCorrectedCommits) {
    let forgeUserId = nullUserId;
    if (timelineEvent.actorId) {
      forgeUserId = memberExternalIdForgeUserIdMap.get(timelineEvent.actorId) || nullUserId;
      timelineEventForgeUsersIdsMap.set(timelineEvent, { actorId: forgeUserId, committerId: nullUserId });
    }

    switch (timelineEvent.type) {
      case 'committed': {
        const committerExternalId = (timelineEvent.data as { committerId: number | null }).committerId;
        const committerEmail = (timelineEvent.data as extract.CommittedEvent).committerEmail;

        const authorExternalId = timelineEvent.actorId;
        const authorEmail = timelineEvent.actorEmail;

        let authorForgeUserId = authorExternalId ? memberExternalIdForgeUserIdMap.get(authorExternalId) : undefined;
        let committerForgeUserId = committerExternalId ? memberExternalIdForgeUserIdMap.get(committerExternalId) : undefined;

        if (!authorForgeUserId && authorEmail) authorForgeUserId = memberEmailForgeUserIdMap.get(authorEmail);
        if (!committerForgeUserId && committerEmail) committerForgeUserId = memberEmailForgeUserIdMap.get(committerEmail);

        timelineEventForgeUsersIdsMap.set(timelineEvent, {
          actorId: authorForgeUserId || nullUserId,
          committerId: committerForgeUserId || nullUserId
        });

        if (authorForgeUserId && !committers.includes(authorForgeUserId)) committers.push(authorForgeUserId);
        if (committerForgeUserId && !committers.includes(committerForgeUserId)) committers.push(committerForgeUserId);
        break;
      }
      case 'reviewed': {
        const forgeUserId =
          timelineEvent.actorId
            ? memberExternalIdForgeUserIdMap.get(timelineEvent.actorId) || nullUserId
            : nullUserId;
        reviewers.push(forgeUserId);
        if ((timelineEvent.data as extract.ReviewedEvent).state === 'approved') {
          approvers.push(forgeUserId);
        }
        break;
      }
      case 'merged': {
        const forgeUserId =
          timelineEvent.actorId
            ? memberExternalIdForgeUserIdMap.get(timelineEvent.actorId) || nullUserId
            : nullUserId;
        mergedBy = forgeUserId;
        break;
      }
    }
  }

  const usersJunk = mapUsersToJunk({
    author,
    mergedBy,
    approvers,
    committers,
    reviewers,
  }, nullUserId);
  /**** MergeRequestUsersJunk end ****/

  /**** MergeRequestEvents ****/
  const mergeRequestEvents: transform.NewMergeRequestEvent[] = [];

  timelineEventsWithCorrectedCommits.forEach((event) => {
    mergeRequestEvents.push({
      repository: transformRepositoryId,
      mergeRequest: transformMergeRequestId,
      timestamp: event.timestamp,
      occuredOn: timestampDateMap.get(event.timestamp.getTime()) || nullDateId,
      commitedAt: event.type === 'committed' ? timestampDateMap.get(new Date((event.data as extract.CommittedEvent).committedDate).getTime()) || nullDateId : nullDateId,
      actor: timelineEventForgeUsersIdsMap.get(event)?.actorId || nullUserId,
      htmlUrl: event.htmlUrl || '',
      subject: nullUserId,
      mergeRequestEventType: event.type,
      reviewState: 'unknown',
    });
  });

  extractData.notes.forEach((note) => {
    mergeRequestEvents.push({
      repository: transformRepositoryId,
      mergeRequest: transformMergeRequestId,
      timestamp: note.timestamp,
      occuredOn: timestampDateMap.get(note.timestamp.getTime()) || nullDateId,
      commitedAt: nullDateId,
      actor: memberExternalIdForgeUserIdMap.get(note.authorExternalId) || nullUserId,
      subject: nullUserId,
      mergeRequestEventType: 'noted',
      reviewState: 'unknown',
      htmlUrl: note.htmlUrl || '',
    });
  });

  const opened = {
    mergeRequest: transformMergeRequestId,
    mergeRequestEventType: 'opened',
    timestamp: extractData.mergeRequest.openedAt || new Date(0),
    occuredOn: openedAtId,
    commitedAt: nullDateId,
    actor: author,
    subject: nullUserId,
    repository: transformRepositoryId,
    reviewState: 'unknown',
  } satisfies transform.NewMergeRequestEvent;

  const startedCoding = {
    mergeRequest: transformMergeRequestId,
    mergeRequestEventType: 'started_coding',
    timestamp: timeline.startedCodingAt || new Date(0),
    occuredOn: startedCodingAtId,
    commitedAt: nullDateId,
    actor: author,
    subject: nullUserId,
    repository: transformRepositoryId,
    reviewState: 'unknown',
  } satisfies transform.NewMergeRequestEvent;

  const startedPickup = {
    mergeRequest: transformMergeRequestId,
    mergeRequestEventType: 'started_pickup',
    timestamp: timeline.startedPickupAt || new Date(0),
    occuredOn: startedPickupAtId,
    commitedAt: nullDateId,
    actor: author,
    subject: nullUserId,
    repository: transformRepositoryId,
    reviewState: 'unknown',
  } satisfies transform.NewMergeRequestEvent;

  const startedReview = {
    mergeRequest: transformMergeRequestId,
    mergeRequestEventType: 'started_review',
    timestamp: timeline.startedReviewAt || new Date(0),
    occuredOn: startedReviewAtId,
    commitedAt: nullDateId,
    actor: author,
    subject: nullUserId,
    repository: transformRepositoryId,
    reviewState: 'unknown',
  } satisfies transform.NewMergeRequestEvent;

  mergeRequestEvents.push(opened, startedCoding, startedPickup, startedReview);
  /**** MergeRequestEvents end ****/

  /**** Deployment start ****/
  const deployedAtId = timestampDateMap.get(deployedAt?.getTime() || 0) || nullDateId;
  const deployDuration = extractData.mergeRequest.mergedAt && deployedAt ? deployedAt.getTime() - extractData.mergeRequest.mergedAt.getTime() : 0;
  if (extractData.deployment) {
    await upsertDeployment(ctx.transformDatabase, {
      deployedAt: deployedAtId,
      deploymentType: extractData.deployment.type,
      externalId: extractData.deployment.externalId,
      repositoryId: transformRepositoryId,
      status: extractData.deployment.status,
    });
  }
  /**** Deployment end ****/


  /**** DB update / insert ****/
  await ctx.transformDatabase.transaction(
    async (tx) => {
      if (mergeRequestMetricsId && datesJunkId && usersJunkId) {
        await updateDateJunk(tx, {
          id: datesJunkId,
          mergedAt: mergedAtId,
          closedAt: closedAtId,
          openedAt: openedAtId,
          startedCodingAt: startedCodingAtId,
          lastUpdatedAt: lastUpdatedAtId,
          startedPickupAt: startedPickupAtId,
          startedReviewAt: startedReviewAtId,
          deployedAt: deployedAtId,
        }).run();

        await updateUserJunk(tx, {
          id: usersJunkId,
          ...usersJunk
        }).run();

        await updateMergeMetrics(tx, {
          id: mergeRequestMetricsId,
          datesJunk: datesJunkId,
          usersJunk: usersJunkId,
          mrSize,
          codeAddition,
          codeDeletion,
          codingDuration: timeline.codingDuration,
          pickupDuration: timeline.pickupDuration,
          reviewDuration: timeline.reviewDuration,
          timeToMerge: timeline.timeToMerge,
          deployDuration,
          handover: timeline.handover,
          reviewDepth: timeline.reviewDepth,
          deployed,
          merged,
          closed,
          approved: timeline.approved,
          reviewed: timeline.reviewed,
          repository: transformRepositoryId,
          mergeRequest: transformMergeRequestId,
        }).run();

      } else {

        const { id: dateJunkId } = await insertDateJunk(tx, {
          mergedAt: mergedAtId,
          closedAt: closedAtId,
          openedAt: openedAtId,
          startedCodingAt: startedCodingAtId,
          lastUpdatedAt: lastUpdatedAtId,
          startedPickupAt: startedPickupAtId,
          startedReviewAt: startedReviewAtId,
          deployedAt: deployedAtId,
        }).get();

        const { id: userJunkId } = await insertUserJunk(tx, usersJunk).get();

        await insertMergeMetrics(tx, {
          mrSize,
          codeAddition,
          codeDeletion,
          codingDuration: timeline.codingDuration,
          pickupDuration: timeline.pickupDuration,
          reviewDuration: timeline.reviewDuration,
          timeToMerge: timeline.timeToMerge,
          deployDuration,
          handover: timeline.handover,
          reviewDepth: timeline.reviewDepth,
          deployed,
          merged,
          closed,
          approved: timeline.approved,
          reviewed: timeline.reviewed,
          usersJunk: userJunkId,
          datesJunk: dateJunkId,
          repository: transformRepositoryId,
          mergeRequest: transformMergeRequestId,
        }).run()

      }

      await deleteMergeRequestEvents(tx, transformMergeRequestId).run();
      await Promise.all(mergeRequestEvents.map(event => insertMergeRequestEvent(tx, event).run()));
    })
  /**** DB update / insert end ****/
}
