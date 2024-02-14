import { describe, expect, test } from "@jest/globals";

import { drizzle } from "drizzle-orm/libsql";
import { migrate } from "drizzle-orm/libsql/migrator";
import { createClient } from "@libsql/client";
import { and, eq, inArray } from "drizzle-orm";
import fs from "fs";

import * as extract from "@dxta/extract-schema";
import * as transform from "@dxta/transform-schema";
import type { Context } from "./config";
import type { SetMergeRequestsExtractEntities, SetMergeRequestsTransformEntities } from "./set-merge-requests";
import { setMergeRequests } from "./set-merge-requests";

let sqlite: ReturnType<typeof createClient>;
let db: ReturnType<typeof drizzle>;
let context: Context<SetMergeRequestsExtractEntities, SetMergeRequestsTransformEntities>;

const dbName = 'set-merge-requests';

beforeAll(async () => {
  sqlite = createClient({
    url: `file:${dbName}`,
  });
  db = drizzle(sqlite);

  await migrate(db, { migrationsFolder: "../../../migrations/tenant-db" });

  context = {
    extract: {
      db,
      entities: {
        repositories: extract.repositories,
        mergeRequests: extract.mergeRequests
      }
    },
    transform: {
      db,
      entities: {
        mergeRequests: transform.mergeRequests
      }
    }
  };
});

afterAll(() => {
  sqlite.close();
  fs.unlinkSync(dbName);
});

beforeEach(async () => {
  await db.insert(extract.namespaces).values([
    { id: 1, externalId: 2000, forgeType: 'github', name: 'crocoder-dev' }
  ]);
  await db.insert(context.extract.entities.repositories).values([
    { id: 1, externalId: 1000, forgeType: 'github', name: 'Repo-repo', namespaceId: 1 }
  ]);
  await db.insert(context.extract.entities.mergeRequests).values([
    { id: 1, canonId: 1, createdAt: new Date(), externalId: 2000, repositoryId: 1, title: "Test", webUrl: "http://localhost/Test" },
    { id: 2, canonId: 2, createdAt: new Date(), externalId: 2001, repositoryId: 1, title: "Test", webUrl: "http://localhost/Test" },
    { id: 3, canonId: 3, createdAt: new Date(), externalId: 2002, repositoryId: 1, title: "Test", webUrl: "http://localhost/Test" },
  ]);
});

afterEach(async () => {
  await db.delete(context.extract.entities.mergeRequests).run();
  await db.delete(context.extract.entities.repositories).run();
  await db.delete(extract.namespaces).run();
  await db.delete(context.transform.entities.mergeRequests).run();
});

describe('set-merge-requests', () => {
  describe('setMergeRequests', () => {
    test('should insert values into db', async () => {
      const extractMergeRequestIds = [1, 2, 3];
      await setMergeRequests({ extractMergeRequestIds }, context);

      const transformMergeRequestRows = await db.select({
        title: context.transform.entities.mergeRequests.title,
        webUrl: context.transform.entities.mergeRequests.webUrl
      }).from(context.transform.entities.mergeRequests)
        .where(
          and(
            inArray(context.transform.entities.mergeRequests.externalId, [2000, 2001, 2002]),
            eq(context.transform.entities.mergeRequests.forgeType, "github")
          )
        ).all();

      expect(transformMergeRequestRows).toEqual([
        { title: "Test", webUrl: "http://localhost/Test" },
        { title: "Test", webUrl: "http://localhost/Test" },
        { title: "Test", webUrl: "http://localhost/Test" },
      ]);
    });
  });
});