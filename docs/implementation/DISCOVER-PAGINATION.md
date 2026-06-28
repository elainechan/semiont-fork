# Discover Pagination — Implementation Plan

**Issue:** Discover page causes OOM crash on large knowledge bases (30K+ resources)

## Root Cause

The issue assumed the backend sends all records in one SSE payload. The backend already paginates at `browser.ts:127-129`. The real OOM is one layer deeper:

```
kb.views.getAll()          ← reads ALL 31K .json files from disk
  → each file = ResourceDescriptor + ALL ResourceAnnotations (annotation blobs)
  → 31K × (metadata + annotation blobs) → heap exhausted
sortByDateDesc(31K items)  ← too late
slice(0, 50)               ← too late
```

Fix: replace `views.getAll()` with `kb.graph.listResources()`. The graph is already fully in memory at startup (that's where the 915K events live). `MemoryGraph` stores a `Map<string, ResourceDescriptor>` — pure metadata, no annotation blobs.

## What Already Exists (no changes needed)

- `BrowseResourcesRequest` schema: has `offset`, `limit`, `search`, `archived`, `entityType`
- `ListResourcesResponse` schema: has `resources`, `total`, `offset`, `limit`
- `Browser.handleBrowseResources`: already slices with offset/limit, returns total
- `MemoryGraph.listResources`: supports offset/limit/entityTypes filtering
- SDK `resourceListCache`: passes `offset: 0` and `limit: filters.limit ?? 100`

## Data Flow

```
BEFORE:
  browse:resources-requested{offset,limit}
  → ResourceContext.listResources()
    → kb.views.getAll()  ← 31K disk reads + parse, includes annotation blobs [OOM]
    → entityType post-filter
    → sortByDateDesc
    → slice(offset, limit)
  → SSE: browse:resources-result

AFTER:
  browse:resources-requested{archived,entityType,search,offset,limit}
  → kb.graph.listResources({archived,entityTypes,search,offset,limit})  ← in-memory
  → if search: addContentPreviews(page)  ← only N items, not 31K
  → SSE: browse:resources-result{resources,total,offset,limit}

FRONTEND ACCUMULATION:
  selectedEntityType$ change → reset offset=0, clear list
  mount / filter change → resourcesPage({offset:0, limit:RECENT_LIMIT})
  loadMore() → resourcesPage({offset:currentLen, limit:RECENT_LIMIT})
             → append to accumulatedResources$
```

## Dependency Graph

```
ResourceFilter (core)
  └── MemoryGraph.listResources (graph)
        └── Browser.handleBrowseResources (make-meaning)
              └── SDK: browse.resourcesPage() [new one-shot method]
                    └── DiscoverStateUnit (react-ui)
                          └── ResourceDiscoveryPage (react-ui)
                                └── discover/page.tsx (frontend)
```

---

## Step 1 — `packages/core/src/resource-types.ts`

Add `archived` to `ResourceFilter`:

```typescript
export interface ResourceFilter {
  entityTypes?: string[];
  search?: string;
  limit?: number;
  offset?: number;
  archived?: boolean;  // ADD
}
```

---

## Step 2 — `packages/graph/src/implementations/memorygraph.ts`

`listResources` at line 105: add archived filter + sort by dateCreated desc.

```typescript
async listResources(filter: ResourceFilter): Promise<{ resources: ResourceDescriptor[]; total: number }> {
  let docs = Array.from(this.resources.values());

  // existing entityTypes + search filters unchanged...

  if (filter.archived !== undefined) {
    docs = docs.filter(d => (d.archived ?? false) === filter.archived);
  }

  // Sort by dateCreated desc (match ResourceContext.sortByDateDesc)
  docs.sort((a, b) => {
    const aT = a.dateCreated ? new Date(a.dateCreated).getTime() : 0;
    const bT = b.dateCreated ? new Date(b.dateCreated).getTime() : 0;
    return bT - aT;
  });

  const total = docs.length;
  const offset = filter.offset ?? 0;
  const limit = filter.limit ?? 20;
  docs = docs.slice(offset, offset + limit);
  return { resources: docs, total };
}
```

Other graph implementations (neo4j, janusgraph, neptune): add `archived` filter or TODO comment if not used in prod.

---

## Step 3 — `packages/make-meaning/src/browser.ts`

Replace `handleBrowseResources` (lines 114–151). Remove `ResourceContext.listResources` + manual entityType filter. Use graph directly:

```typescript
private async handleBrowseResources(event: EventMap['browse:resources-requested']): Promise<void> {
  try {
    const limit = Math.min(event.limit ?? 50, 500);
    const offset = event.offset ?? 0;

    const { resources: page, total } = await this.kb.graph.listResources({
      search: event.search,
      archived: event.archived,
      entityTypes: event.entityType ? [event.entityType] : undefined,
      limit,
      offset,
    });

    const formattedDocs = event.search
      ? await ResourceContext.addContentPreviews(page, this.kb)
      : page;

    this.eventBus.get('browse:resources-result').next({
      correlationId: event.correlationId,
      response: { resources: formattedDocs, total, offset, limit },
    });
  } catch (error) {
    this.logger.error('Browse resources failed', { error: errField(error) });
    this.eventBus.get('browse:resources-failed').next({
      correlationId: event.correlationId,
      message: error instanceof Error ? error.message : String(error),
    });
  }
}
```

`ResourceContext.addContentPreviews` is still used — called on the paged result (N items), not all 31K.

---

## Step 4 — `packages/sdk/src/namespaces/browse.ts`

Add `resourcesPage()` one-shot method after `resourceContent` (line 286). Existing `resources()` cache is unchanged (backward compat).

```typescript
async resourcesPage(filters?: ResourceListFilters & { offset?: number }): Promise<{
  resources: ResourceDescriptor[];
  total: number;
  offset: number;
  limit: number;
}> {
  const search = filters?.search ? searchQuery(filters.search) : undefined;
  return busRequest<{ resources: ResourceDescriptor[]; total: number; offset: number; limit: number }>(
    this.transport,
    'browse:resources-requested',
    {
      search,
      archived: filters?.archived,
      entityType: filters?.entityType,
      limit: filters?.limit ?? 50,
      offset: filters?.offset ?? 0,
    },
    'browse:resources-result',
    'browse:resources-failed',
  );
}
```

---

## Step 5 — `packages/react-ui/src/features/resource-discovery/state/discover-state-unit.ts`

Extend interface:

```typescript
const RECENT_LIMIT = 50;  // bump from 10

export interface DiscoverStateUnit extends StateUnit {
  browse: ShellStateUnit;
  search: DiscoverSearchPipeline;
  recentResources$: Observable<ResourceDescriptor[]>;
  recentTotal$: Observable<number>;       // ADD
  hasMoreRecent$: Observable<boolean>;    // ADD
  isLoadingMore$: Observable<boolean>;    // ADD
  entityTypes$: Observable<string[]>;
  isLoadingRecent$: Observable<boolean>;
  selectedEntityType$: Observable<string>;
  setSelectedEntityType(value: string): void;
  loadMoreRecent(): void;                 // ADD
}
```

Replace `recent$` switchMap with BehaviorSubject accumulation:

```typescript
const accumulatedResources$ = new BehaviorSubject<ResourceDescriptor[]>([]);
const recentTotal$ = new BehaviorSubject<number>(0);
const isLoadingRecent$ = new BehaviorSubject<boolean>(true);
const isLoadingMore$ = new BehaviorSubject<boolean>(false);

disposer.add(
  selectedEntityType$.pipe(
    switchMap((et) => {
      accumulatedResources$.next([]);
      recentTotal$.next(0);
      isLoadingRecent$.next(true);
      return from(client.browse.resourcesPage({
        limit: RECENT_LIMIT, archived: false, offset: 0,
        ...(et ? { entityType: et } : {}),
      }));
    }),
  ).subscribe(({ resources, total }) => {
    accumulatedResources$.next(resources);
    recentTotal$.next(total);
    isLoadingRecent$.next(false);
  })
);

const hasMoreRecent$: Observable<boolean> = combineLatest([accumulatedResources$, recentTotal$]).pipe(
  map(([resources, total]) => resources.length < total),
);

const loadMoreRecent = async () => {
  const et = selectedEntityType$.value;
  const offset = accumulatedResources$.value.length;
  isLoadingMore$.next(true);
  const { resources, total } = await client.browse.resourcesPage({
    limit: RECENT_LIMIT, archived: false, offset,
    ...(et ? { entityType: et } : {}),
  });
  accumulatedResources$.next([...accumulatedResources$.value, ...resources]);
  recentTotal$.next(total);
  isLoadingMore$.next(false);
};
```

---

## Step 6 — `packages/react-ui/src/features/resource-discovery/components/ResourceDiscoveryPage.tsx`

Add props:

```typescript
export interface ResourceDiscoveryPageProps {
  // ...existing...
  recentTotal?: number;
  hasMoreRecent?: boolean;
  isLoadingMore?: boolean;
  onLoadMoreRecent?: () => void;
  translations: {
    // ...existing...
    loadMore: string;
    resourceCount: (n: number) => string;
  };
}
```

In render — show total count in header when not searching and total > 0:

```tsx
{!hasSearchQuery && recentTotal !== undefined && recentTotal > 0 && (
  <span className="semiont-card__documents-count">
    {t.resourceCount(recentTotal)}
  </span>
)}
```

Show load-more button below the grid when not searching:

```tsx
{!hasSearchQuery && hasMoreRecent && (
  <button
    onClick={onLoadMoreRecent}
    disabled={isLoadingMore}
    className="semiont-card__load-more"
  >
    {isLoadingMore ? t.searching : t.loadMore}
  </button>
)}
```

---

## Step 7 — `apps/frontend/src/app/[locale]/know/discover/page.tsx`

Wire new observables and pass to component:

```typescript
const recentTotal = useObservable(stateUnit.recentTotal$) ?? 0;
const hasMoreRecent = useObservable(stateUnit.hasMoreRecent$) ?? false;
const isLoadingMore = useObservable(stateUnit.isLoadingMore$) ?? false;
```

Add to `<ResourceDiscoveryPage>`:
```tsx
recentTotal={recentTotal}
hasMoreRecent={hasMoreRecent}
isLoadingMore={isLoadingMore}
onLoadMoreRecent={stateUnit.loadMoreRecent}
translations={{
  // ...existing...
  loadMore: t('loadMore'),
  resourceCount: (n) => t('resourceCount', { count: n }),
}}
```

---

## Acceptance Criteria Map

| Criterion | Steps |
|---|---|
| No OOM with 31K resources at default 1.5 GB heap | 1–3 |
| First paint < 2s (first 50 resources) | 3–5 |
| Filter change resets to page 1 | 5 (selectedEntityType$ resets accumulation) |
| Total count in header ("31,844 resources") | 5–7 |
| Load more button hidden on last page | 5–7 (hasMoreRecent$ = false) |
| Backward compat (callers omitting offset/limit get first 50) | 3 (defaults) |
| Search/filter still works | unchanged (search path uses graph.searchResources) |

## Out of Scope

- Neo4j/JanusGraph/Neptune `archived` filter implementation (add TODO)
- `views.getAll()` in `handleBrowseDirectory` (separate call site, different use case)
- Search result pagination (search already works per-keystroke with limit=20)
- Persistent graph store to reduce startup replay time (separate issue)
