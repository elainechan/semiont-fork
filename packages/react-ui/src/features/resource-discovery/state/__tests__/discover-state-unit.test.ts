import { describe, it, expect, vi } from 'vitest';
import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { filter, skip, take, toArray } from 'rxjs/operators';
import type { SemiontClient } from '@semiont/sdk';
import type { ShellStateUnit } from '../../../../state/shell-state-unit';
import { createDiscoverStateUnit } from '../discover-state-unit';

function mockBrowse(): ShellStateUnit {
  return { dispose: vi.fn() } as unknown as ShellStateUnit;
}

interface BrowseFilters {
  limit?: number;
  archived?: boolean;
  search?: string;
  entityType?: string;
  offset?: number;
}

type PageResult = { resources: unknown[]; total: number; offset: number; limit: number };

function mockClient(overrides: {
  resourcesPageFn?: (filters: BrowseFilters) => Promise<PageResult>;
  entityTypes$?: BehaviorSubject<string[] | undefined>;
  resourcesFn?: (filters: BrowseFilters) => BehaviorSubject<unknown[] | undefined>;
} = {}): { client: SemiontClient; pagesCalls: BrowseFilters[]; resourceCalls: BrowseFilters[] } {
  const pagesCalls: BrowseFilters[] = [];
  const resourceCalls: BrowseFilters[] = [];

  const entityTypes$ =
    overrides.entityTypes$ ?? new BehaviorSubject<string[] | undefined>(['Person']);

  const defaultPageFn: (filters: BrowseFilters) => Promise<PageResult> =
    (f) => Promise.resolve({ resources: [{ '@id': 'r1' }], total: 1, offset: f.offset ?? 0, limit: f.limit ?? 50 });
  const resourcesPageFn = overrides.resourcesPageFn ?? defaultPageFn;

  const defaultResourcesFn = () => new BehaviorSubject<unknown[] | undefined>([]);
  const resourcesFn = overrides.resourcesFn ?? defaultResourcesFn;

  const client = {
    browse: {
      resourcesPage: (filters: BrowseFilters = {}) => {
        pagesCalls.push(filters);
        return resourcesPageFn(filters);
      },
      resources: (filters: BrowseFilters = {}) => {
        resourceCalls.push(filters);
        return resourcesFn(filters).asObservable();
      },
      entityTypes: () => entityTypes$.asObservable(),
    },
  } as unknown as SemiontClient;

  return { client, pagesCalls, resourceCalls };
}

describe('createDiscoverStateUnit', () => {
  it('exposes recent resources from resourcesPage on mount', async () => {
    const { client } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const recent = await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));
    expect(recent).toEqual([{ '@id': 'r1' }]);

    stateUnit.dispose();
  });

  it('exposes entity types from browse namespace', async () => {
    const { client } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const types = await firstValueFrom(stateUnit.entityTypes$);
    expect(types).toEqual(['Person']);

    stateUnit.dispose();
  });

  it('falls back to [] when entityTypes() emits undefined', async () => {
    const entityTypes$ = new BehaviorSubject<string[] | undefined>(undefined);
    const { client } = mockClient({ entityTypes$ });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const types = await firstValueFrom(stateUnit.entityTypes$);
    expect(types).toEqual([]);

    stateUnit.dispose();
  });

  it('reports loading true initially and false after page loads', async () => {
    let resolve!: (v: PageResult) => void;
    const pending = new Promise<PageResult>((res) => { resolve = res; });
    const { client } = mockClient({ resourcesPageFn: () => pending });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    expect(await firstValueFrom(stateUnit.isLoadingRecent$)).toBe(true);

    resolve({ resources: [], total: 0, offset: 0, limit: 50 });
    const loaded = await firstValueFrom(stateUnit.isLoadingRecent$.pipe(filter((l) => !l)));
    expect(loaded).toBe(false);

    stateUnit.dispose();
  });

  it('exposes a search pipeline', () => {
    const { client } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    expect(stateUnit.search).toBeDefined();
    expect(typeof stateUnit.search.setQuery).toBe('function');
    expect(stateUnit.search.state$).toBeDefined();

    stateUnit.dispose();
  });

  it('disposes browse and subjects on dispose', () => {
    const browse = mockBrowse();
    const { client } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, browse);
    stateUnit.dispose();

    expect(browse.dispose).toHaveBeenCalled();
  });

  it('initial selectedEntityType$ is empty and recent fetch carries no entityType', async () => {
    const { client, pagesCalls } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for first page to load
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));

    expect(pagesCalls).toHaveLength(1);
    expect(pagesCalls[0]).toEqual({ limit: 50, archived: false, offset: 0 });
    expect(await firstValueFrom(stateUnit.selectedEntityType$)).toBe('');

    stateUnit.dispose();
  });

  it('setSelectedEntityType resets list and refetches with entityType filter', async () => {
    const { client, pagesCalls } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for initial load
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));

    stateUnit.setSelectedEntityType('Person');

    // Wait for refetch
    await new Promise((res) => setTimeout(res, 10));

    expect(pagesCalls.length).toBeGreaterThanOrEqual(2);
    expect(pagesCalls.at(-1)).toEqual({ limit: 50, archived: false, offset: 0, entityType: 'Person' });

    stateUnit.dispose();
  });

  it('recentTotal$ reflects total from resourcesPage response', async () => {
    const { client } = mockClient({
      resourcesPageFn: () => Promise.resolve({ resources: [{ '@id': 'r1' }], total: 999, offset: 0, limit: 50 }),
    });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const total = await firstValueFrom(stateUnit.recentTotal$.pipe(filter((t) => t > 0)));
    expect(total).toBe(999);

    stateUnit.dispose();
  });

  it('hasMoreRecent$ is true when resources.length < total', async () => {
    const { client } = mockClient({
      resourcesPageFn: () => Promise.resolve({ resources: [{ '@id': 'r1' }], total: 100, offset: 0, limit: 50 }),
    });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const hasMore = await firstValueFrom(stateUnit.hasMoreRecent$.pipe(filter(Boolean)));
    expect(hasMore).toBe(true);

    stateUnit.dispose();
  });

  it('hasMoreRecent$ is false when all resources are loaded', async () => {
    const { client } = mockClient({
      resourcesPageFn: () => Promise.resolve({ resources: [{ '@id': 'r1' }], total: 1, offset: 0, limit: 50 }),
    });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for load
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));
    const hasMore = await firstValueFrom(stateUnit.hasMoreRecent$);
    expect(hasMore).toBe(false);

    stateUnit.dispose();
  });

  it('loadMoreRecent appends next page to recentResources$', async () => {
    let call = 0;
    const { client } = mockClient({
      resourcesPageFn: (f) => {
        call++;
        if (call === 1) return Promise.resolve({ resources: [{ '@id': 'r1' }], total: 2, offset: 0, limit: 50 });
        return Promise.resolve({ resources: [{ '@id': 'r2' }], total: 2, offset: 1, limit: 50 });
      },
    });
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for initial load
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));

    stateUnit.loadMoreRecent();

    const all = await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length >= 2)));
    expect(all.map((r: any) => r['@id'])).toEqual(['r1', 'r2']);

    stateUnit.dispose();
  });

  it('filter change resets accumulated resources and fetches page 0', async () => {
    const { client, pagesCalls } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for initial load
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));

    stateUnit.setSelectedEntityType('Article');
    // After reset, recentResources$ should briefly be []
    // then populate after refetch
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));

    const lastCall = pagesCalls.at(-1)!;
    expect(lastCall.offset).toBe(0);
    expect(lastCall.entityType).toBe('Article');

    stateUnit.dispose();
  });

  it('search with an empty query yields no results without hitting resourcesPage', async () => {
    vi.useFakeTimers();
    try {
      const { client, pagesCalls, resourceCalls } = mockClient();
      const stateUnit = createDiscoverStateUnit(client, mockBrowse());

      const collected: Array<{ results: unknown[]; isSearching: boolean }> = [];
      const sub = stateUnit.search.state$.subscribe((s) => collected.push(s));

      await vi.advanceTimersByTimeAsync(300);

      expect(collected.at(-1)).toEqual({ results: [], isSearching: false });
      const searchCalls = resourceCalls.filter((c) => c.search !== undefined);
      expect(searchCalls).toHaveLength(0);

      sub.unsubscribe();
      stateUnit.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it('search with a non-empty query pushes search and entityType into resources filter', async () => {
    vi.useFakeTimers();
    try {
      const results$ = new BehaviorSubject<unknown[] | undefined>([{ '@id': 'hit' }]);
      const { client, resourceCalls } = mockClient({
        resourcesFn: (filters) => (filters.search ? results$ : new BehaviorSubject<unknown[] | undefined>([])),
      });
      const stateUnit = createDiscoverStateUnit(client, mockBrowse());

      const sub = stateUnit.search.state$.subscribe();
      stateUnit.setSelectedEntityType('Person');
      stateUnit.search.setQuery('lincoln');

      await vi.advanceTimersByTimeAsync(300);

      const searchCalls = resourceCalls.filter((c) => c.search !== undefined);
      expect(searchCalls.length).toBeGreaterThanOrEqual(1);
      expect(searchCalls.at(-1)).toEqual({
        search: 'lincoln',
        limit: 20,
        entityType: 'Person',
      });

      sub.unsubscribe();
      stateUnit.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it('search results flow through state$ once the debounced query fetches', async () => {
    vi.useFakeTimers();
    try {
      const results$ = new BehaviorSubject<unknown[] | undefined>([{ '@id': 'hit' }]);
      const { client } = mockClient({
        resourcesFn: () => results$,
      });
      const stateUnit = createDiscoverStateUnit(client, mockBrowse());

      const collected: Array<{ results: unknown[]; isSearching: boolean }> = [];
      const sub = stateUnit.search.state$.subscribe((s) => collected.push(s));

      stateUnit.search.setQuery('lincoln');
      await vi.advanceTimersByTimeAsync(300);

      expect(collected.at(-1)).toEqual({ results: [{ '@id': 'hit' }], isSearching: false });

      sub.unsubscribe();
      stateUnit.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it('search reports isSearching while the fetch is in flight', async () => {
    vi.useFakeTimers();
    try {
      const inflight$ = new BehaviorSubject<unknown[] | undefined>(undefined);
      const { client } = mockClient({
        resourcesFn: () => inflight$,
      });
      const stateUnit = createDiscoverStateUnit(client, mockBrowse());

      const collected: Array<{ results: unknown[]; isSearching: boolean }> = [];
      const sub = stateUnit.search.state$.subscribe((s) => collected.push(s));

      stateUnit.search.setQuery('lincoln');
      await vi.advanceTimersByTimeAsync(300);

      expect(collected.at(-1)).toEqual({ results: [], isSearching: true });

      sub.unsubscribe();
      stateUnit.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it('search query observable echoes the latest setQuery value', async () => {
    const { client } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    const queries = stateUnit.search.query$.pipe(skip(1), take(1), toArray()).toPromise();
    stateUnit.search.setQuery('alpha');

    expect(await queries).toEqual(['alpha']);

    stateUnit.dispose();
  });

  it('omits entityType from the filter when the empty sentinel is selected', async () => {
    const { client, pagesCalls } = mockClient();
    const stateUnit = createDiscoverStateUnit(client, mockBrowse());

    // Wait for initial load then switch entity type twice
    await firstValueFrom(stateUnit.recentResources$.pipe(filter((r) => r.length > 0)));
    stateUnit.setSelectedEntityType('Person');
    await new Promise((res) => setTimeout(res, 10));
    stateUnit.setSelectedEntityType('');
    await new Promise((res) => setTimeout(res, 10));

    const last = pagesCalls.at(-1)!;
    expect(last.entityType).toBeUndefined();

    stateUnit.dispose();
  });
});
