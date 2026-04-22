use cel_interpreter::Program;

pub fn compile_rules(rules: &[String], policy_name: &str) -> Result<Vec<Program>, String> {
    rules
        .iter()
        .enumerate()
        .map(|(index, rule)| {
            Program::compile(rule).map_err(|e| {
                format!(
                    "Failed to compile {policy_name} rule #{} '{rule}': {e}",
                    index + 1
                )
            })
        })
        .collect()
}
