import { BehaviorSubject, Subject, combineLatest, of, from, type Observable } from 'rxjs';
import { debounceTime, distinctUntilChanged, map, startWith, switchMap, shareReplay } from 'rxjs/operators';
import type { ResourceDescriptor } from '@semiont/core';
import type { SemiontClient } from '@semiont/sdk';
import type { StateUnit } from '@semiont/core';
import { createDisposer } from '@semiont/sdk';
import type { ShellStateUnit } from '../../../state/shell-state-unit';

const RECENT_LIMIT = 50;
const SEARCH_LIMIT = 20;
const DEBOUNCE_MS = 250;

export interface DiscoverSearchPipeline {
  query$: Observable<string>;
  state$: Observable<{ results: ResourceDescriptor[]; isSearching: boolean }>;
  setQuery(value: string): void;
}

export interface DiscoverStateUnit extends StateUnit {
  browse: ShellStateUnit;
  search: DiscoverSearchPipeline;
  recentResources$: Observable<ResourceDescriptor[]>;
  recentTotal$: Observable<number>;
  hasMoreRecent$: Observable<boolean>;
  isLoadingMore$: Observable<boolean>;
  entityTypes$: Observable<string[]>;
  isLoadingRecent$: Observable<boolean>;
  selectedEntityType$: Observable<string>;
  setSelectedEntityType(value: string): void;
  loadMoreRecent(): void;
}

export function createDiscoverStateUnit(
  client: SemiontClient,
  browse: ShellStateUnit,
): DiscoverStateUnit {
  const disposer = createDisposer();
  // `browse` (ShellStateUnit) is a *passed-in* dependency owned by the caller
  // (`useShellStateUnit`), not this unit — do NOT add it to the disposer (it's the
  // shared, app-scoped shell). See packages/sdk/docs/STATE-UNITS.md (composition rule).

  const selectedEntityType$ = new BehaviorSubject<string>('');
  disposer.add(() => selectedEntityType$.complete());

  const queryInput$ = new Subject<string>();
  disposer.add(() => queryInput$.complete());

  // Accumulated recent-resources state
  const accumulatedResources$ = new BehaviorSubject<ResourceDescriptor[]>([]);
  const recentTotal$ = new BehaviorSubject<number>(0);
  const isLoadingRecent$ = new BehaviorSubject<boolean>(true);
  const isLoadingMore$ = new BehaviorSubject<boolean>(false);

  disposer.add(() => accumulatedResources$.complete());
  disposer.add(() => recentTotal$.complete());
  disposer.add(() => isLoadingRecent$.complete());
  disposer.add(() => isLoadingMore$.complete());

  // Filter change → reset and fetch page 0
  const recentSub = selectedEntityType$.pipe(
    switchMap((et) => {
      accumulatedResources$.next([]);
      recentTotal$.next(0);
      isLoadingRecent$.next(true);
      return from(client.browse.resourcesPage({
        limit: RECENT_LIMIT,
        archived: false,
        offset: 0,
        ...(et ? { entityType: et } : {}),
      }));
    }),
  ).subscribe({
    next: ({ resources, total }) => {
      accumulatedResources$.next(resources);
      recentTotal$.next(total);
      isLoadingRecent$.next(false);
    },
    error: () => {
      isLoadingRecent$.next(false);
    },
  });
  disposer.add(() => recentSub.unsubscribe());

  const hasMoreRecent$: Observable<boolean> = combineLatest([accumulatedResources$, recentTotal$]).pipe(
    map(([resources, total]) => resources.length < total),
  );

  const loadMoreRecent = () => {
    if (isLoadingMore$.value || isLoadingRecent$.value) return;
    const et = selectedEntityType$.value;
    const offset = accumulatedResources$.value.length;
    isLoadingMore$.next(true);
    client.browse.resourcesPage({
      limit: RECENT_LIMIT,
      archived: false,
      offset,
      ...(et ? { entityType: et } : {}),
    }).then(({ resources, total }) => {
      accumulatedResources$.next([...accumulatedResources$.value, ...resources]);
      recentTotal$.next(total);
      isLoadingMore$.next(false);
    }).catch(() => {
      isLoadingMore$.next(false);
    });
  };

  const entityTypes$: Observable<string[]> = client.browse.entityTypes().pipe(
    map((e) => e ?? []),
  );

  const debouncedQuery$ = queryInput$.pipe(
    startWith(''),
    debounceTime(DEBOUNCE_MS),
    distinctUntilChanged(),
  );

  const state$: Observable<{ results: ResourceDescriptor[]; isSearching: boolean }> =
    combineLatest([debouncedQuery$, selectedEntityType$]).pipe(
      switchMap(([q, et]) => {
        const trimmed = q.trim();
        if (!trimmed) {
          return of({ results: [] as ResourceDescriptor[], isSearching: false });
        }
        return client.browse
          .resources({
            search: trimmed,
            limit: SEARCH_LIMIT,
            ...(et ? { entityType: et } : {}),
          })
          .pipe(
            map((results) => ({
              results: results ?? [],
              isSearching: results === undefined,
            })),
            startWith({ results: [] as ResourceDescriptor[], isSearching: true }),
          );
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
    );

  const search: DiscoverSearchPipeline = {
    query$: queryInput$.pipe(startWith('')),
    state$,
    setQuery: (value) => queryInput$.next(value),
  };

  return {
    browse,
    search,
    recentResources$: accumulatedResources$.asObservable(),
    recentTotal$: recentTotal$.asObservable(),
    hasMoreRecent$,
    isLoadingMore$: isLoadingMore$.asObservable(),
    entityTypes$,
    isLoadingRecent$: isLoadingRecent$.asObservable(),
    selectedEntityType$: selectedEntityType$.asObservable(),
    setSelectedEntityType: (value) => selectedEntityType$.next(value),
    loadMoreRecent,
    dispose: () => disposer.dispose(),
  };
}
