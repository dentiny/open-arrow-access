# OpenStax

Enable `service-openstax`. `OpenStaxService::new()?.book("osbooks-physics","main")`
resolves the selected branch/tag to a commit SHA, then exposes the GitHub source
archive as `osbooks-physics.zip`. Downloads use that immutable commit ID.
Metadata keeps the repository name, commit SHA and complete parsed commit metadata.
The original archive retains CNXML modules, collections, licenses and tracked media.
Compose the existing ZIP component to read selected archived objects.
Git submodules and LFS pointer files are retained as supplied by GitHub's archive;
they are not silently fetched from other repositories or expanded.

`repositories(page,per_page)` returns one unfiltered public OpenStax organization
page. The caller selects osbooks repositories, advances pages, and records the
snapshot. Public access needs no key; an optional GitHub token can be configured
on an explicit API Connection. Archive connections have independent configuration.
Rate limits, retries, source conversion and Lance ingestion remain caller-owned.

Sources: [OpenStax repositories](https://github.com/openstax),
[GitHub archives](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-tar).

ZIP is selected deliberately: GitHub TAR archives include PAX headers that our TAR reader does not support. Downloads are sequential; materialize a selected archive in caller-owned ranged storage before opening it with `ZipArchive`.
