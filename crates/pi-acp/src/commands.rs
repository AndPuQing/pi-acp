//! Slash commands — file-based (user/project `.md`) loading and expansion.
//!
//! Ports `acp/slash-commands.ts`: frontmatter parsing, `$1`/`$@` argument
//! substitution, and the user/project `prompts/**/*.md` scan. S6 (W-453) adds
//! the built-in commands (`/compact` `/autocompact` ...) and skill commands on
//! top of this layer.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use agent_client_protocol::schema::v1::AvailableCommand;

use crate::settings::agent_dir;

/// A file-based slash command (mirrors pi-coding-agent semantics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSlashCommand {
    /// Command name without the leading slash.
    pub name: String,
    pub description: String,
    /// Prompt template body (frontmatter stripped).
    pub content: String,
    /// e.g. `(user)`, `(project)`, `(project:frontend)`.
    pub source: String,
}

/// Where a file command was discovered (drives the `source` label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandSource {
    User,
    Project,
}

impl CommandSource {
    fn label(self, subdir: &str) -> String {
        match (self, subdir.is_empty()) {
            (CommandSource::User, true) => "(user)".to_string(),
            (CommandSource::User, false) => format!("(user:{subdir})"),
            (CommandSource::Project, true) => "(project)".to_string(),
            (CommandSource::Project, false) => format!("(project:{subdir})"),
        }
    }
}

/// Parse YAML-ish frontmatter off a command file.
///
/// Mirrors TS `parseFrontmatter`: a leading `---` block terminated by a line
/// starting with `---`; only `key: value` lines are collected; the remainder
/// (trimmed) is the command body.
pub fn parse_frontmatter(content: &str) -> (HashMap<String, String>, String) {
    let mut frontmatter = HashMap::new();

    if !content.starts_with("---") {
        return (frontmatter, content.to_string());
    }

    let Some(relative_end) = content[3..].find("\n---") else {
        return (frontmatter, content.to_string());
    };
    let end_index = 3 + relative_end;

    let frontmatter_block = &content[4..end_index];
    let remaining = content[end_index + 4..].trim();

    for line in frontmatter_block.lines() {
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim();
            if !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                frontmatter.insert(key.to_string(), value.trim().to_string());
            }
        }
    }

    (frontmatter, remaining.to_string())
}

/// Recursively load `.md` command files from a directory.
///
/// Mirrors TS `loadCommandsFromDir`: subdirectories are traversed (their name
/// becomes the `(user:sub)` / `(project:sub)` label segment), unreadable files
/// and dirs are silently skipped.
pub fn load_commands_from_dir(
    dir: &Path,
    source: CommandSource,
    subdir: &str,
) -> Vec<FileSlashCommand> {
    let mut commands = Vec::new();

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return commands,
    };

    for entry in entries.flatten() {
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        let full_path = entry.path();

        if file_type.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            let new_subdir = if subdir.is_empty() {
                name
            } else {
                format!("{subdir}:{name}")
            };
            commands.extend(load_commands_from_dir(&full_path, source, &new_subdir));
            continue;
        }

        if !file_type.is_file() || !full_path.extension().is_some_and(|e| e == "md") {
            continue;
        }

        let Ok(raw_content) = fs::read_to_string(&full_path) else {
            continue;
        };
        let (frontmatter, content) = parse_frontmatter(&raw_content);

        let name = full_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let source_str = source.label(subdir);

        let mut description = frontmatter.get("description").cloned().unwrap_or_default();
        if description.is_empty() {
            if let Some(first_line) = content.lines().find(|l| !l.trim().is_empty()) {
                description = first_line.chars().take(60).collect();
                if first_line.chars().count() > 60 {
                    description.push_str("...");
                }
            }
        }

        description = if description.is_empty() {
            source_str.clone()
        } else {
            format!("{description} {source_str}")
        };

        commands.push(FileSlashCommand {
            name,
            description,
            content,
            source: source_str,
        });
    }

    commands
}

/// Load file commands from pi's prompt directories, user first then project
/// (mirrors TS `loadSlashCommands`):
/// - user:    `~/.pi/agent/prompts/**/*.md` (honoring `PI_CODING_AGENT_DIR`);
/// - project: `<cwd>/.pi/prompts/**/*.md`.
pub fn load_slash_commands(cwd: &Path) -> Vec<FileSlashCommand> {
    load_slash_commands_at(&agent_dir(), cwd)
}

/// [`load_slash_commands`] with an explicit agent dir (testable / injectable).
pub fn load_slash_commands_at(agent_dir: &Path, cwd: &Path) -> Vec<FileSlashCommand> {
    let mut commands = Vec::new();
    let user_dir = agent_dir.join("prompts");
    let project_dir = cwd.join(".pi").join("prompts");
    commands.extend(load_commands_from_dir(&user_dir, CommandSource::User, ""));
    commands.extend(load_commands_from_dir(
        &project_dir,
        CommandSource::Project,
        "",
    ));
    commands
}

/// Convert file commands to ACP `AvailableCommand`s, de-duping by name (first
/// wins, so user commands shadow project ones).
pub fn to_available_commands(file_commands: &[FileSlashCommand]) -> Vec<AvailableCommand> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for c in file_commands {
        if !seen.insert(c.name.as_str()) {
            continue;
        }
        out.push(AvailableCommand::new(&c.name, &c.description));
    }
    out
}

/// Parse command args bash-style (single/double quotes, whitespace-separated).
/// Mirrors TS `parseCommandArgs`.
pub fn parse_command_args(args_string: &str) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;

    for ch in args_string.chars() {
        match in_quote {
            Some(q) => {
                if ch == q {
                    in_quote = None;
                } else {
                    current.push(ch);
                }
            }
            None => {
                if ch == '"' || ch == '\'' {
                    in_quote = Some(ch);
                } else if ch == ' ' || ch == '\t' {
                    if !current.is_empty() {
                        args.push(std::mem::take(&mut current));
                    }
                } else {
                    current.push(ch);
                }
            }
        }
    }

    if !current.is_empty() {
        args.push(current);
    }
    args
}

/// Substitute `$1`, `$2`, ... and `$@` in a command body with the parsed args.
/// Mirrors TS `substituteArgs` (`$@` first, then positional; missing positions
/// become empty strings).
pub fn substitute_args(content: &str, args: &[String]) -> String {
    let joined = args.join(" ");
    let after_all = content.replace("$@", &joined);

    // Positional `$N` (1-based). Replace left-to-right so `$1` inside a longer
    // token like `$10` is not partially matched.
    let mut result = String::with_capacity(after_all.len());
    let mut rest = after_all.as_str();
    while let Some(pos) = rest.find('$') {
        let before = &rest[..pos];
        let after = &rest[pos + 1..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            result.push_str(before);
            result.push('$');
            rest = after;
            continue;
        }
        let index = digits.parse::<usize>().unwrap_or(0).saturating_sub(1);
        result.push_str(before);
        if let Some(arg) = args.get(index) {
            result.push_str(arg);
        }
        rest = &after[digits.len()..];
    }
    result.push_str(rest);
    result
}

/// Expand a leading `/command` using the loaded file commands.
///
/// Mirrors TS `expandSlashCommand`: returns the original text when the text is
/// not a slash command or names an unknown command.
pub fn expand_slash_command(text: &str, file_commands: &[FileSlashCommand]) -> String {
    if !text.starts_with('/') {
        return text.to_string();
    }

    let (command_name, args_string) = match text[1..].find(' ') {
        Some(space) => (&text[1..1 + space], &text[1 + space + 1..]),
        None => (&text[1..], ""),
    };

    let Some(cmd) = file_commands.iter().find(|c| c.name == command_name) else {
        return text.to_string();
    };

    let args = parse_command_args(args_string);
    substitute_args(&cmd.content, &args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // --- parse_frontmatter ---

    #[test]
    fn frontmatter_parses_key_values() {
        let (fm, body) = parse_frontmatter("---\ndescription: Do a thing\n---\nbody line");
        assert_eq!(
            fm.get("description").map(String::as_str),
            Some("Do a thing")
        );
        assert_eq!(body, "body line");
    }

    #[test]
    fn frontmatter_missing_is_whole_body() {
        let (fm, body) = parse_frontmatter("just a body");
        assert!(fm.is_empty());
        assert_eq!(body, "just a body");
    }

    #[test]
    fn frontmatter_unterminated_is_whole_body() {
        let (fm, body) = parse_frontmatter("---\ndescription: x\nno closing");
        assert!(fm.is_empty());
        assert_eq!(body, "---\ndescription: x\nno closing");
    }

    #[test]
    fn frontmatter_ignores_non_key_value_lines() {
        let (fm, body) =
            parse_frontmatter("---\n# comment\ndescription:  hi  \nweird line\n---\nbody");
        assert_eq!(fm.get("description").map(String::as_str), Some("hi"));
        assert_eq!(body, "body");
    }

    #[test]
    fn frontmatter_requires_dash_prefix_of_first_line() {
        let (fm, _) = parse_frontmatter("x---\ndescription: y\n---\nbody");
        assert!(fm.is_empty());
    }

    // --- parse_command_args ---

    #[test]
    fn args_split_on_whitespace() {
        assert_eq!(parse_command_args("a b  c"), vec!["a", "b", "c"]);
        assert_eq!(parse_command_args(""), Vec::<String>::new());
        assert_eq!(parse_command_args("   "), Vec::<String>::new());
        assert_eq!(parse_command_args("tab\tsep"), vec!["tab", "sep"]);
    }

    #[test]
    fn args_respect_quotes() {
        assert_eq!(
            parse_command_args("say \"hello world\" now"),
            vec!["say", "hello world", "now"]
        );
        assert_eq!(
            parse_command_args("'single' \"double\""),
            vec!["single", "double"]
        );
        assert_eq!(parse_command_args("a\"b c\"d"), vec!["ab cd"]);
        // unterminated quote consumes the rest
        assert_eq!(
            parse_command_args("open \"unterminated"),
            vec!["open", "unterminated"]
        );
    }

    // --- substitute_args ---

    #[test]
    fn substitute_positional_and_all() {
        let args = vec!["one".to_string(), "two three".to_string()];
        assert_eq!(
            substitute_args("see $1 and $2", &args),
            "see one and two three"
        );
        assert_eq!(substitute_args("all: $@", &args), "all: one two three");
        assert_eq!(substitute_args("missing $3", &args), "missing ");
        assert_eq!(substitute_args("no args $1", &[]), "no args ");
    }

    #[test]
    fn substitute_handles_adjacent_and_large_indices() {
        let args = vec!["a".to_string(), "b".to_string()];
        assert_eq!(substitute_args("$1$2", &args), "ab");
        // `$10` must not partially match `$1`
        assert_eq!(substitute_args("x$10y", &args), "xy");
        // bare `$` without digits stays
        assert_eq!(substitute_args("cost is $", &args), "cost is $");
    }

    // --- expand_slash_command ---

    fn cmds() -> Vec<FileSlashCommand> {
        vec![
            FileSlashCommand {
                name: "greet".into(),
                description: "Greet".into(),
                content: "Hello, $1! You said: $@".into(),
                source: "(user)".into(),
            },
            FileSlashCommand {
                name: "plain".into(),
                description: "Plain".into(),
                content: "static body".into(),
                source: "(project)".into(),
            },
        ]
    }

    #[test]
    fn expand_known_command() {
        let expanded = expand_slash_command("/greet World hi there", &cmds());
        assert_eq!(expanded, "Hello, World! You said: World hi there");
        assert_eq!(expand_slash_command("/plain", &cmds()), "static body");
    }

    #[test]
    fn expand_unknown_or_non_slash_returns_original() {
        assert_eq!(expand_slash_command("/nope x", &cmds()), "/nope x");
        assert_eq!(expand_slash_command("plain text", &cmds()), "plain text");
        assert_eq!(expand_slash_command("", &cmds()), "");
        assert_eq!(expand_slash_command("/", &cmds()), "/");
    }

    // --- fs loading ---

    #[test]
    fn load_commands_from_dir_recurses_and_skips_non_md() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("frontend")).unwrap();
        fs::write(
            dir.path().join("build.md"),
            "---\ndescription: Build it\n---\nRun the build",
        )
        .unwrap();
        fs::write(dir.path().join("notes.txt"), "not a command").unwrap();
        fs::write(
            dir.path().join("frontend").join("deploy.md"),
            "Deploy steps here",
        )
        .unwrap();

        let cmds = load_commands_from_dir(dir.path(), CommandSource::Project, "");
        let names: Vec<&str> = cmds.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"build"));
        assert!(names.contains(&"deploy"));
        assert!(!names.contains(&"notes"));

        let build = cmds.iter().find(|c| c.name == "build").unwrap();
        assert_eq!(build.description, "Build it (project)");
        assert_eq!(build.content, "Run the build");
        assert_eq!(build.source, "(project)");

        let deploy = cmds.iter().find(|c| c.name == "deploy").unwrap();
        // first-line fallback
        assert_eq!(deploy.description, "Deploy steps here (project:frontend)");
        assert_eq!(deploy.source, "(project:frontend)");
    }

    #[test]
    fn description_fallback_truncates_at_60_chars() {
        let dir = TempDir::new().unwrap();
        let long_line = "x".repeat(80);
        fs::write(dir.path().join("long.md"), long_line.clone()).unwrap();

        let cmds = load_commands_from_dir(dir.path(), CommandSource::User, "");
        assert_eq!(cmds.len(), 1);
        let desc = &cmds[0].description;
        assert_eq!(desc, &format!("{}... (user)", "x".repeat(60)));
    }

    #[test]
    fn load_slash_commands_at_user_first_then_project() {
        let agent = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        fs::create_dir_all(agent.path().join("prompts")).unwrap();
        fs::create_dir_all(project.path().join(".pi").join("prompts")).unwrap();
        fs::write(agent.path().join("prompts").join("a.md"), "user a").unwrap();
        fs::write(agent.path().join("prompts").join("b.md"), "user b").unwrap();
        fs::write(
            project.path().join(".pi").join("prompts").join("b.md"),
            "project b",
        )
        .unwrap();
        fs::write(
            project.path().join(".pi").join("prompts").join("c.md"),
            "project c",
        )
        .unwrap();

        let cmds = load_slash_commands_at(agent.path(), project.path());
        // All user commands precede all project commands; within a source the
        // readdir order is filesystem-dependent, so compare as sorted lists.
        let sources: Vec<(&str, &str)> = cmds
            .iter()
            .map(|c| (c.name.as_str(), c.source.as_str()))
            .collect();
        let split = sources
            .iter()
            .position(|(_, s)| s.starts_with("(project)"))
            .unwrap_or(sources.len());
        let (user, project_cmds) = sources.split_at(split);
        let mut user = user.to_vec();
        let mut project_cmds = project_cmds.to_vec();
        user.sort();
        project_cmds.sort();
        assert_eq!(user, vec![("a", "(user)"), ("b", "(user)")]);
        assert_eq!(project_cmds, vec![("b", "(project)"), ("c", "(project)")]);
    }

    #[test]
    fn missing_dirs_load_nothing() {
        let dir = TempDir::new().unwrap();
        assert!(
            load_commands_from_dir(&dir.path().join("nope"), CommandSource::User, "").is_empty()
        );
    }

    #[test]
    fn to_available_commands_dedupes_first_wins() {
        let dup = vec![
            FileSlashCommand {
                name: "same".into(),
                description: "first".into(),
                content: "".into(),
                source: "(user)".into(),
            },
            FileSlashCommand {
                name: "same".into(),
                description: "second".into(),
                content: "".into(),
                source: "(project)".into(),
            },
            FileSlashCommand {
                name: "other".into(),
                description: "other".into(),
                content: "".into(),
                source: "(user)".into(),
            },
        ];
        let available = to_available_commands(&dup);
        assert_eq!(available.len(), 2);
        assert_eq!(available[0].name, "same");
        assert_eq!(available[0].description, "first");
        assert_eq!(available[1].name, "other");
    }
}
