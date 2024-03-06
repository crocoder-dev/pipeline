import type { InferSelectModel, InferInsertModel } from 'drizzle-orm';
import { sql } from 'drizzle-orm';
import { integer  } from 'drizzle-orm/sqlite-core';
import { sqliteTable } from './transform-table';

export const nullRows = sqliteTable('null_rows', {
  id: integer('id').primaryKey(),
  dateId: integer('dates_id').notNull(),
  userId: integer('users_id').notNull(),
  mergeRequestId: integer('merge_requests_id').notNull(),
  repositoryId: integer('repository_id').notNull(),
  branchId: integer('branch_id').notNull().default(sql`1`),
  _createdAt: integer('__created_at', { mode: 'timestamp' }).default(sql`(strftime('%s', 'now'))`),
  _updatedAt: integer('__updated_at', { mode: 'timestamp' }).default(sql`(strftime('%s', 'now'))`),
});

export type NullRows = InferSelectModel<typeof nullRows>;
export type NewNullRows = InferInsertModel<typeof nullRows>;
