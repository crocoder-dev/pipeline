import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import type { LibSQLDatabase } from 'drizzle-orm/libsql';
import type { repositories, namespaces, mergeRequests, members, repositoriesToMembers, mergeRequestDiffs, mergeRequestCommits } from '@acme/extract-schema';
import type { SourceControl } from '@acme/source-control';

export type Database = BetterSQLite3Database | LibSQLDatabase;

export type Entities = {
  repositories: typeof repositories;
  namespaces: typeof namespaces;
  mergeRequests: typeof mergeRequests;
  mergeRequestDiffs: typeof mergeRequestDiffs;
  members: typeof members;
  repositoriesToMembers: typeof repositoriesToMembers;
  mergeRequestCommits: typeof mergeRequestCommits;
};

export type Context<SC extends Partial<SourceControl>, E extends Partial<Entities>> = {
  integrations: {
    sourceControl: SC | null;
  };
  db: Database;
  entities: E;
};

export type Input = Record<string, unknown>;

export type ExtractFunction<I extends Input, O, SC extends Partial<SourceControl>, E extends Partial<Entities>> = (input: I, context: Context<SC, E>) => Promise<O>;
