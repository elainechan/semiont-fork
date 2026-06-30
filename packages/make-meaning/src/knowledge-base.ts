/**
 * Knowledge Base
 *
 * The durable store that records what intelligent actors decide.
 * Groups the KB subsystems from ARCHITECTURE.md:
 *
 * - Event Log (immutable append-only) — via EventStore
 * - Materialized Views (fast single-doc queries) — via ViewStorage
 * - Content Store (working-tree files, URI-addressed) — via WorkingTreeStore
 * - Graph (eventually consistent relationship projection) — via GraphDatabase
 * - Graph Consumer (event-to-graph projection) — via GraphDBConsumer
 * - Vectors (semantic search) — via VectorStore (optional, read-only)
 *
 * The Smelter (event-to-vector projection) runs as an external actor
 * via @semiont/make-meaning/smelter-main. It subscribes to domain events
 * via the EventBus gateway, embeds content, and writes to Qdrant directly.
 */

import * as path from 'path';
import type { EventStore } from '@semiont/event-sourcing';
import { FilesystemViewStorage, type ViewStorage } from '@semiont/event-sourcing';
import { WorkingTreeStore } from '@semiont/content';
import type { GraphDatabase } from '@semiont/graph';
import { MemoryGraphDatabase } from '@semiont/graph';
import type { VectorStore } from '@semiont/vectors';
import type { SemiontProject } from '@semiont/core/node';
import type { EventBus, Logger } from '@semiont/core';
import { GraphDBConsumer } from './graph/consumer.js';

export interface KnowledgeBase {
  eventStore:    EventStore;
  views:         ViewStorage;
  content:       WorkingTreeStore;
  graph:         GraphDatabase;
  graphConsumer: GraphDBConsumer;
  vectors?:      VectorStore;
  projectionsDir: string;
}

export interface CreateKnowledgeBaseOptions {
  vectorStore?: VectorStore;
  skipRebuild?: boolean;
}

export async function createKnowledgeBase(
  eventStore: EventStore,
  project: SemiontProject,
  graphDb: GraphDatabase,
  eventBus: EventBus,
  logger: Logger,
  options?: CreateKnowledgeBaseOptions,
): Promise<KnowledgeBase> {
  const views = new FilesystemViewStorage(project, logger.child({ component: 'view-storage' }));
  const content = new WorkingTreeStore(
    project,
    logger.child({ component: 'working-tree-store' }),
  );
  const graphConsumer = new GraphDBConsumer(
    eventStore,
    graphDb,
    eventBus,
    logger.child({ component: 'graph-consumer' }),
  );
  await graphConsumer.initialize();

  if (!options?.skipRebuild) {
    // Rebuild materialized views from the event log first. The Browser actor
    // reads from these views, so they must be populated before any request is
    // served.
    await eventStore.views.rebuildAll(eventStore.log);

    // Graph rebuild — use snapshot if available for fast incremental startup.
    // Snapshot stores serialized graph state + timestamp of last included event.
    // On warm boot: load snapshot → replay only resources changed since snapshot → save updated snapshot.
    // On cold boot (no snapshot): full rebuild → save snapshot for next boot.
    const snapshotPath = path.join(project.projectionsDir, 'graph-snapshot.json');
    let snapshotTime: Date | null = null;

    if (graphDb instanceof MemoryGraphDatabase) {
      snapshotTime = await graphDb.loadSnapshot(snapshotPath);
    }

    if (snapshotTime) {
      logger.info('Graph snapshot found — running incremental rebuild', {
        snapshotTime: snapshotTime.toISOString(),
      });
      await graphConsumer.rebuildIncremental(snapshotTime);
    } else {
      logger.info('No graph snapshot — running full rebuild');
      await graphConsumer.rebuildAll();
    }

    if (graphDb instanceof MemoryGraphDatabase) {
      await graphDb.saveSnapshot(snapshotPath);
    }
  }

  const kb: KnowledgeBase = {
    eventStore, views, content, graph: graphDb, graphConsumer,
    projectionsDir: project.projectionsDir,
  };

  if (options?.vectorStore) {
    kb.vectors = options.vectorStore;
  }

  return kb;
}
