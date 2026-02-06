/**
 * An asynchronous iterable wrapper for paginated data that
 * allows iteration over pages or individual items.
 *
 * It can fetch and iterate over each individual element in all the pages lazily.
 */
export interface PagedAsyncIterable<TItem, TPage> extends AsyncIterable<TItem> {
    /**
     * An asynchronous iterable of Pages, that can fetch and
     * iterate over each page lazily.
     */
    pages: AsyncIterable<TPage>;
}
//# sourceMappingURL=pagedAsyncIterable.d.ts.map