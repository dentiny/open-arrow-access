//! Internal continuation discovery, independent of Arrow decoding and scan execution.
use std::{collections::HashSet, hash::Hash, num::NonZeroUsize};

use open_arrow_access_core::{Error, Result, async_trait};

/// A provider response and its validated continuation. Empty bodies may continue.
pub(crate) struct Page<T, C> {
    pub(crate) body: T,
    pub(crate) next: Option<C>,
}

/// Fetch one continuation; initial selectors and protocol validation stay with the provider.
#[async_trait]
pub(crate) trait PageSource: Send + Sync {
    type Cursor: Clone + Eq + Hash + Send;
    type Body: Send;

    async fn resume_page(&self, cursor: Self::Cursor) -> Result<Page<Self::Body, Self::Cursor>>;
}

/// Preserve each provider's existing error category, operation and message.
pub(crate) struct PaginationErrors {
    pub(crate) repeated_cursor: fn() -> Error,
    pub(crate) page_limit: fn() -> Error,
}

/// Continuation state for one scan, after its first page has already been fetched.
/// Callers control demand and decide when the current page has been consumed.
pub(crate) struct Pager<S: PageSource> {
    source: S,
    next: Option<S::Cursor>,
    seen: HashSet<S::Cursor>,
    pages: usize,
    max_pages: Option<NonZeroUsize>,
    errors: PaginationErrors,
}

impl<S: PageSource> Pager<S> {
    pub(crate) fn new(
        source: S,
        next: Option<S::Cursor>,
        resumed_from: Option<S::Cursor>,
        max_pages: Option<NonZeroUsize>,
        errors: PaginationErrors,
    ) -> Self {
        Self {
            source,
            next,
            seen: resumed_from.into_iter().collect(),
            pages: 1,
            max_pages,
            errors,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<S::Body>> {
        let Some(cursor) = self.next.take() else {
            return Ok(None);
        };
        // Keep the original check order: repeated cursors take precedence over limits.
        if !self.seen.insert(cursor.clone()) {
            return Err((self.errors.repeated_cursor)());
        }
        if self
            .max_pages
            .is_some_and(|limit| self.pages >= limit.get())
        {
            return Err((self.errors.page_limit)());
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(self.errors.page_limit)?;
        let page = self.source.resume_page(cursor).await?;
        self.next = page.next;
        Ok(Some(page.body))
    }
}
