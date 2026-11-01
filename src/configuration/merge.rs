use toml::Spanned;
use toml::de::{DeTable, DeValue};

/// Merge `overlay` into `base`, letting `overlay` win. Tables merge recursively
/// so a later file can add keys to a table an earlier file opened; every other
/// value replaces wholesale, arrays included.
pub fn merge<'i>(base: &mut DeTable<'i>, overlay: DeTable<'i>) {
    for (key, overlay_value) in overlay {
        let value = match base.remove(key.get_ref().as_ref()) {
            Some(base_value) => merge_value(base_value, overlay_value),
            None => overlay_value,
        };
        base.insert(key, value);
    }
}

/// Spans of a merged tree are never rendered, because an error spanning several
/// documents has no single source to quote, so the base span is kept as-is.
fn merge_value<'i>(
    base: Spanned<DeValue<'i>>,
    overlay: Spanned<DeValue<'i>>,
) -> Spanned<DeValue<'i>> {
    let span = base.span();
    match (base.into_inner(), overlay.into_inner()) {
        (DeValue::Table(mut base_table), DeValue::Table(overlay_table)) => {
            merge(&mut base_table, overlay_table);
            Spanned::new(span, DeValue::Table(base_table))
        }
        (_, overlay_value) => Spanned::new(span, overlay_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged(base: &str, overlay: &str) -> String {
        let mut base_table = DeTable::parse(base).expect("base parses").into_inner();
        let overlay_table = DeTable::parse(overlay)
            .expect("overlay parses")
            .into_inner();
        merge(&mut base_table, overlay_table);
        format!("{base_table:?}")
    }

    #[test]
    fn overlay_adds_key_to_existing_table() {
        let result = merged("[s3]\nbucket = \"b\"\n", "[s3]\nregion = \"r\"\n");
        assert!(result.contains("bucket"), "base key kept: {result}");
        assert!(result.contains("region"), "overlay key added: {result}");
    }

    #[test]
    fn overlay_scalar_replaces_base_scalar() {
        let result = merged("[s3]\nbucket = \"base\"\n", "[s3]\nbucket = \"overlay\"\n");
        assert!(result.contains("\"overlay\""), "overlay wins: {result}");
        assert!(!result.contains("\"base\""), "base value gone: {result}");
    }

    #[test]
    fn overlay_array_replaces_rather_than_appends() {
        let result = merged("rules = [\"a\", \"b\"]\n", "rules = [\"c\"]\n");
        assert!(result.contains("\"c\""), "overlay array present: {result}");
        assert!(
            !result.contains("\"a\"") && !result.contains("\"b\""),
            "base array fully replaced: {result}"
        );
    }

    #[test]
    fn nested_tables_merge_at_every_depth() {
        let result = merged(
            "[metadata_store.s3]\nbucket = \"b\"\n",
            "[metadata_store.s3.lock_strategy.redis]\nurl = \"redis://x\"\n",
        );
        assert!(result.contains("bucket"), "deep base key kept: {result}");
        assert!(result.contains("redis://x"), "deep overlay added: {result}");
    }
}
