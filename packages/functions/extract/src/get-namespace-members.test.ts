import { drizzle } from "drizzle-orm/libsql";
import { migrate } from "drizzle-orm/libsql/migrator";
import { createClient } from '@libsql/client';

import { describe, expect, test } from '@jest/globals';
import { getNamespaceMembers } from './get-namespace-members';

import { members, repositories, repositoriesToMembers } from '@dxta/extract-schema';
import type { Context } from './config';
import type { GetNamespaceMembersSourceControl, GetNamespaceMembersEntities } from './get-namespace-members';
import type { SourceControl } from '@dxta/source-control';
import fs from 'fs';
import { namespaces } from "@dxta/extract-schema/src/namespaces";

let sqlite: ReturnType<typeof createClient>;
let db: ReturnType<typeof drizzle>;
let context: Context<GetNamespaceMembersSourceControl, GetNamespaceMembersEntities>;
let fetchNamespaceMembers: SourceControl['fetchNamespaceMembers'];

const databaseName = 'fetch-namespace-members.db';

beforeAll(async () => {
  sqlite = createClient({
    url: `file:${databaseName}`,
  });
  db = drizzle(sqlite);

  await migrate(db, { migrationsFolder: "../../../migrations/tenant-db" });
 
  await db.insert(namespaces).values({
    id: 1,
    name: 'crocoder-dev',
    externalId: 2000,
    forgeType: 'github',
  }).run();

  await db.insert(repositories).values({
    id: 1,
    namespaceId: 1,
    name: 'pipeline',
    externalId: 1000,
    forgeType: 'github',
  }).run();

  fetchNamespaceMembers = jest.fn((_externalNamespaceId, namespaceName, perPage: number, page?: number ) => {
    switch (namespaceName) {
      case 'crocoder-dev':
        return Promise.resolve({
          members: [
            { externalId: 1000, name: 'Dejan', username: 'dejan-crocoder', forgeType: 'github' }
          ],
          pagination: {
            page: page || 1,
            perPage,
            totalPages: 1,
          }
        }) satisfies ReturnType<SourceControl['fetchNamespaceMembers']>;
      default:
        return Promise.reject(new Error("Are you mocking me?"))
    }
  });

  context = {
    entities: { members, repositoriesToMembers },
    db,
    integrations: {
      sourceControl: {
        fetchNamespaceMembers,
      }
    }
  };

});



afterAll(() => {
  sqlite.close();
  fs.unlinkSync(databaseName);
});

describe('get-namespace-members:', () => {
  describe('getNamespaceMembers', () => {
    test('should insert member data in the database', async () => {
      const { members, paginationInfo } = await getNamespaceMembers({
        externalNamespaceId: 1000,
        namespaceName: 'crocoder-dev',
        repositoryId: 1,
        perPage: 1000
      }, context);

      expect(members).toBeDefined();
      expect(paginationInfo).toBeDefined();
      expect(fetchNamespaceMembers).toHaveBeenCalledTimes(1);

      const memberRows = await db.select().from(context.entities.members).all();
      expect(memberRows.length).toEqual(members.length);

      for (const memberRow of memberRows) {
        const rowMatchedMember = members.find((m) => m.externalId === memberRow.externalId);
        expect(rowMatchedMember).toBeDefined();
        if (!rowMatchedMember) return;
        expect(rowMatchedMember.id).toEqual(memberRow.id);
      }

    });
  });
});

