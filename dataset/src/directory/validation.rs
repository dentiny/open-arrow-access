use glob::Pattern;
use open_arrow_access_core::Result;

pub(super) struct ResolvedGlob<'a> {
    pub prefix: &'a str,
    pub pattern: Option<Pattern>,
}

pub(super) fn resolve_glob(path: &str) -> Result<ResolvedGlob<'_>> {
    let Some(wildcard) = path.find(['*', '?', '[']) else {
        return Ok(ResolvedGlob {
            prefix: path,
            pattern: None,
        });
    };
    let split = path[..wildcard].rfind('/').map_or(0, |slash| slash + 1);
    let (prefix, pattern) = path.split_at(split);
    let pattern = Pattern::new(pattern).map_err(super::error::glob_error)?;
    Ok(ResolvedGlob {
        prefix,
        pattern: Some(pattern),
    })
}
