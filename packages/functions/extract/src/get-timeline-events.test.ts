import { drizzle } from "drizzle-orm/libsql";
import { migrate } from "drizzle-orm/libsql/migrator";
import { createClient } from '@libsql/client';

import type { Context } from "./config";
import { type GetTimelineEventsEntities, type GetTimelineEventsSourceControl, getTimelineEvents } from "./get-timeline-events";
import { namespaces, repositories, mergeRequests, timelineEvents } from "@acme/extract-schema";
import type { Repository, Namespace, MergeRequest, NewRepository, NewNamespace, NewMergeRequest } from "@acme/extract-schema";
import fs from 'fs';

let sqlite: ReturnType<typeof createClient>;
let db: ReturnType<typeof drizzle>;
let context: Context<GetTimelineEventsSourceControl, GetTimelineEventsEntities>;
let fetchTimelineEvents: jest.MockedFunction<GetTimelineEventsSourceControl['fetchTimelineEvents']>;

const TEST_REPO_1 = { id: 1, externalId: 1000, name: 'TEST_REPO_NAME', forgeType: 'github' } satisfies NewRepository;
const TEST_NAMESPACE_1 = { id: 1, externalId: 2000, name: 'TEST_NAMESPACE_NAME', forgeType: 'github' } satisfies NewNamespace;
const TEST_MERGE_REQUEST_1 = { id: 1, externalId: 3000, createdAt: new Date(), canonId: 1, repositoryId: 1, title: "TEST_MR", webUrl: "localhost" } satisfies NewMergeRequest;

const dbname = 'get-merge-request-commits';

beforeAll(async () => {
  sqlite = createClient({
    url: `file:${dbname}`,
  });
  db = drizzle(sqlite);

  await migrate(db, { migrationsFolder: "../../../migrations/extract" });

  await db.insert(repositories).values([TEST_REPO_1]).run();
  await db.insert(namespaces).values([TEST_NAMESPACE_1]).run();
  await db.insert(mergeRequests).values([TEST_MERGE_REQUEST_1]).run();

  fetchTimelineEvents = jest.fn((repository: Repository, namespace: Namespace, mergeRequest: MergeRequest): ReturnType<GetTimelineEventsSourceControl['fetchTimelineEvents']> => {
    switch (mergeRequest.externalId) {
      case 3000:
        return Promise.resolve({
          timelineEvents: [
            {
              external_id: '12345',
              type: 'committed',
              mergeRequestId: 1,
              timestamp: new Date('2023-01-02'),
              actorName: 'MOCK ACTOR',
              actorId: 123,
              actorEmail: 'ACTOR EMAIL',
              data: '{"committerEmail":"COMMITTER EMAIL","committerName":"COMMITTER NAME","committedDate":"2023-01-02T00:00:00.000Z"}',
            },
            {
              external_id: 'sha_value',
              type: 'closed',
              mergeRequestId: 1,
              timestamp: new Date('2023-01-30'),
              actorName: 'MOCK ACTOR',
              actorId: 123,
              actorEmail: 'ACTOR EMAIL',
              data: undefined,
            },
          ]
        });
      default:
        return Promise.reject(new Error('Are you mocking me?'));
    }
  });

  context = {
    db,
    entities: {
      timelineEvents,
      namespaces,
      repositories,
      mergeRequests
    },
    integrations: {
      sourceControl: { fetchTimelineEvents }
    }
  }
});

afterAll(() => {
  sqlite.close();
  fs.unlinkSync(dbname);
});

describe('get-timeline-events:', () => {
  describe('getTimelineEvents', () => {
    test('should insert timeline event data into db', async () => {
      const { timelineEvents } = await getTimelineEvents({
        repositoryId: TEST_REPO_1.id,
        namespaceId: TEST_NAMESPACE_1.id,
        mergeRequestId: TEST_MERGE_REQUEST_1.id
      }, context);

      expect(timelineEvents).toBeDefined();
      expect(fetchTimelineEvents).toHaveBeenCalledTimes(1);

      const timelineEventsRows = await db.select().from(context.entities.timelineEvents).all();
      expect(timelineEventsRows).toHaveLength(2);

      for (const timelineEvent of timelineEventsRows) {
        expect(timelineEvents.find(te => te.id === timelineEvent.id)).toBeDefined();
        expect(timelineEvents.find(te => te.external_id === timelineEvent.external_id)).toBeDefined();
        expect(timelineEvents.find(te => te.mergeRequestId === timelineEvent.mergeRequestId)).toBeDefined();
      }
    });
  })
})