//! Active series custom trackers and cost attribution trackers.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::Value;

#[derive(Clone, Debug)]
pub enum MatchOp {
    Equal(String),
    NotEqual(String),
    Regex(Regex),
    NotRegex(Regex),
}

#[derive(Clone, Debug)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
}

impl LabelMatcher {
    pub fn matches(&self, value: &str) -> bool {
        match &self.op {
            MatchOp::Equal(expected) => value == expected,
            MatchOp::NotEqual(expected) => value != expected,
            MatchOp::Regex(regex) => regex.is_match(value),
            MatchOp::NotRegex(regex) => !regex.is_match(value),
        }
    }
}

/// Parses an Alertmanager-style matcher list such as `{a="b", c=~"d|e"}`, the syntax of Mimir's
/// custom tracker definitions. Braces are optional and values may be unquoted.
pub fn parse_matchers(input: &str) -> Result<Vec<LabelMatcher>> {
    let mut text = input.trim();
    if let Some(inner) = text.strip_prefix('{') {
        text = inner
            .strip_suffix('}')
            .with_context(|| format!("unbalanced braces in {input:?}"))?;
    }
    let mut chars = text.char_indices().peekable();
    let mut matchers = Vec::new();
    loop {
        while chars
            .next_if(|(_, c)| c.is_whitespace() || *c == ',')
            .is_some()
        {}
        let Some(&(start, _)) = chars.peek() else {
            break;
        };
        while chars
            .next_if(|(_, c)| c.is_alphanumeric() || *c == '_' || *c == '.' || *c == ':')
            .is_some()
        {}
        let end = chars.peek().map_or(text.len(), |(index, _)| *index);
        let name = text[start..end].to_owned();
        if name.is_empty() {
            bail!("expected a label name in {input:?}");
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let mut op = String::new();
        while let Some((_, c)) = chars.next_if(|(_, c)| matches!(c, '=' | '!' | '~')) {
            op.push(c);
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let value = if chars.next_if(|(_, c)| *c == '"').is_some() {
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some((_, '"')) => break,
                    Some((_, '\\')) => match chars.next() {
                        Some((_, 'n')) => value.push('\n'),
                        Some((_, 't')) => value.push('\t'),
                        Some((_, c)) => value.push(c),
                        None => bail!("unterminated escape in {input:?}"),
                    },
                    Some((_, c)) => value.push(c),
                    None => bail!("unterminated quoted value in {input:?}"),
                }
            }
            value
        } else {
            let mut value = String::new();
            while let Some((_, c)) = chars.next_if(|(_, c)| *c != ',') {
                value.push(c);
            }
            value.trim_end().to_owned()
        };
        let anchored = |pattern: &str| Regex::new(&format!("^(?:{pattern})$"));
        let op = match op.as_str() {
            "=" => MatchOp::Equal(value),
            "!=" => MatchOp::NotEqual(value),
            "=~" => MatchOp::Regex(anchored(&value)?),
            "!~" => MatchOp::NotRegex(anchored(&value)?),
            other => bail!("unknown match operator {other:?} in {input:?}"),
        };
        matchers.push(LabelMatcher { name, op });
    }
    if matchers.is_empty() {
        bail!("no matchers in {input:?}");
    }
    Ok(matchers)
}

/// A series' labels as the trackers read them.
pub trait LabelSet {
    /// The value of `name`, empty when absent.
    fn value(&self, name: &str) -> &str;
    fn for_each(&self, visit: impl FnMut(&str, &str));
}

/// Pairs sorted by name.
impl<N: AsRef<str>, V: AsRef<str>> LabelSet for [(N, V)] {
    fn value(&self, name: &str) -> &str {
        self.binary_search_by(|(label, _)| label.as_ref().cmp(name))
            .map_or("", |index| self[index].1.as_ref())
    }

    fn for_each(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, value) in self {
            visit(name.as_ref(), value.as_ref());
        }
    }
}

impl<N: AsRef<str>, V: AsRef<str>> LabelSet for Vec<(N, V)> {
    fn value(&self, name: &str) -> &str {
        self.as_slice().value(name)
    }

    fn for_each(&self, visit: impl FnMut(&str, &str)) {
        self.as_slice().for_each(visit)
    }
}

impl LabelSet for crate::labels::Labels {
    fn value(&self, name: &str) -> &str {
        crate::labels::Labels::value(self, name)
    }

    fn for_each(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, value) in self {
            visit(name, value);
        }
    }
}

fn label_value<'a, L: LabelSet + ?Sized>(labels: &'a L, name: &str) -> &'a str {
    labels.value(name)
}

/// Custom trackers, sorted by name. A series matches a tracker when it matches all its matchers.
#[derive(Default, Debug)]
pub struct CustomTrackers {
    names: Vec<String>,
    sources: Vec<String>,
    matchers: Vec<Vec<LabelMatcher>>,
    // Trackers that need a given label value, so a series only evaluates trackers that can match.
    by_value: HashMap<(String, String), Vec<u16>>,
    unindexed: Vec<u16>,
}

impl CustomTrackers {
    pub fn new(sources: BTreeMap<String, String>) -> Result<Self> {
        if sources.len() > usize::from(u16::MAX) {
            bail!("too many custom trackers");
        }
        let mut trackers = Self::default();
        for (index, (name, source)) in sources.into_iter().enumerate() {
            let matchers = parse_matchers(&source)
                .with_context(|| format!("can't build active series matcher {name}"))?;
            let index = index as u16;
            match index_values(&matchers) {
                Some((label, values)) => {
                    for value in values {
                        trackers
                            .by_value
                            .entry((label.clone(), value))
                            .or_default()
                            .push(index);
                    }
                }
                None => trackers.unindexed.push(index),
            }
            trackers.names.push(name);
            trackers.sources.push(source);
            trackers.matchers.push(matchers);
        }
        Ok(trackers)
    }

    /// Parses `<name>:<matcher>[;<name>:<matcher>]*`.
    pub fn parse_flag(flag: &str) -> Result<Vec<(String, String)>> {
        if flag.trim().is_empty() {
            return Ok(Vec::new());
        }
        flag.split(';')
            .map(|pair| {
                let (name, matcher) = pair.split_once(':').with_context(|| {
                    format!("value should be <name>:<matcher>, but colon was not found in {pair:?}")
                })?;
                let (name, matcher) = (name.trim(), matcher.trim());
                if name.is_empty() || matcher.is_empty() {
                    bail!("one side of {pair:?} is empty");
                }
                Ok((name.to_owned(), matcher.to_owned()))
            })
            .collect()
    }

    /// A YAML map of tracker name to matcher.
    pub fn from_value(value: &Value) -> Result<Self> {
        let map = match value {
            Value::Object(map) => map,
            Value::Null => return Ok(Self::default()),
            other => bail!("custom trackers must be a map, got {other}"),
        };
        Self::new(
            map.iter()
                .map(|(name, matcher)| {
                    let matcher = matcher
                        .as_str()
                        .with_context(|| format!("matcher for {name} must be a string"))?;
                    Ok((name.clone(), matcher.to_owned()))
                })
                .collect::<Result<_>>()?,
        )
    }

    /// `self` with `other`'s trackers added, replacing any with the same name.
    pub fn merged(&self, other: &Self) -> Self {
        let mut sources = self
            .names
            .iter()
            .cloned()
            .zip(self.sources.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        sources.extend(
            other
                .names
                .iter()
                .cloned()
                .zip(other.sources.iter().cloned()),
        );
        Self::new(sources).expect("merging valid trackers")
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The indices of the trackers `labels` (sorted by name) matches, in ascending order.
    pub fn matching<L: LabelSet + ?Sized>(&self, labels: &L) -> Vec<u16> {
        let mut candidates = self.unindexed.clone();
        if !self.by_value.is_empty() {
            let mut key = (String::new(), String::new());
            labels.for_each(|name, value| {
                key.0.clear();
                key.0.push_str(name);
                key.1.clear();
                key.1.push_str(value);
                if let Some(indices) = self.by_value.get(&key) {
                    candidates.extend_from_slice(indices);
                }
            });
        }
        candidates.sort_unstable();
        candidates.dedup();
        candidates.retain(|index| {
            self.matchers[usize::from(*index)]
                .iter()
                .all(|matcher| matcher.matches(label_value(labels, &matcher.name)))
        });
        candidates
    }
}

// The label values a series must have for the tracker to match, when one matcher pins them: an
// equality, or a regex that is a plain alternation of literals like `a|b|c`.
fn index_values(matchers: &[LabelMatcher]) -> Option<(String, Vec<String>)> {
    for matcher in matchers {
        if let MatchOp::Equal(value) = &matcher.op
            && !value.is_empty()
        {
            return Some((matcher.name.clone(), vec![value.clone()]));
        }
    }
    for matcher in matchers {
        if let MatchOp::Regex(regex) = &matcher.op {
            let pattern = regex.as_str();
            let inner = &pattern[4..pattern.len() - 2];
            if !inner.is_empty()
                && inner
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '|' | '-'))
                && inner.split('|').all(|value| !value.is_empty())
            {
                return Some((
                    matcher.name.clone(),
                    inner.split('|').map(str::to_owned).collect(),
                ));
            }
        }
    }
    None
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributionLabel {
    pub input: String,
    pub output: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CostAttributionTracker {
    pub name: String,
    pub labels: Vec<AttributionLabel>,
    /// Internal trackers are exposed with the ingester's own metrics, others on the cost
    /// attribution registry path.
    pub internal: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CostAttributionTrackers {
    pub trackers: Vec<CostAttributionTracker>,
}

impl CostAttributionTrackers {
    pub fn from_value(value: &Value) -> Result<Self> {
        let map = match value {
            Value::Object(map) => map,
            Value::Null => return Ok(Self::default()),
            other => bail!("cost attribution trackers must be a map, got {other}"),
        };
        let mut trackers = map
            .iter()
            .map(|(name, config)| {
                let labels = config
                    .get("labels")
                    .and_then(Value::as_array)
                    .with_context(|| format!("tracker {name} has no labels"))?
                    .iter()
                    .map(|label| {
                        let input = label
                            .get("input")
                            .and_then(Value::as_str)
                            .with_context(|| format!("tracker {name} has a label without input"))?;
                        let output = label
                            .get("output")
                            .and_then(Value::as_str)
                            .filter(|output| !output.is_empty())
                            .unwrap_or(input);
                        Ok(AttributionLabel {
                            input: input.to_owned(),
                            output: output.to_owned(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(CostAttributionTracker {
                    name: name.clone(),
                    labels,
                    internal: config
                        .get("internal")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        trackers.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Self { trackers })
    }

    pub fn is_empty(&self) -> bool {
        self.trackers.is_empty()
    }

    pub fn merged(&self, other: &Self) -> Self {
        let mut trackers = self
            .trackers
            .iter()
            .map(|tracker| (tracker.name.clone(), tracker.clone()))
            .collect::<BTreeMap<_, _>>();
        for tracker in &other.trackers {
            trackers.insert(tracker.name.clone(), tracker.clone());
        }
        Self {
            trackers: trackers.into_values().collect(),
        }
    }
}

pub const MISSING_VALUE: &str = "__missing__";
pub const OVERFLOW_VALUE: &str = "__overflow__";

impl CostAttributionTracker {
    /// The attribution values of `labels` (sorted by name), `__missing__` for absent labels.
    pub fn key<L: LabelSet + ?Sized>(&self, labels: &L) -> Vec<String> {
        self.labels
            .iter()
            .map(|label| {
                let value = label_value(labels, &label.input);
                if value.is_empty() {
                    MISSING_VALUE.to_owned()
                } else {
                    value.to_owned()
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut labels = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<Vec<_>>();
        labels.sort();
        labels
    }

    #[test]
    fn parses_tracker_matchers() {
        let matchers =
            parse_matchers(r#"{telemetry_sdk_name="beyla", __name__=~"a|b", job!~"x.*",z!="",}"#)
                .unwrap();
        assert_eq!(matchers.len(), 4);
        assert!(matchers[1].matches("a") && !matchers[1].matches("ab"));
        assert!(matchers[2].matches("y") && !matchers[2].matches("xy"));
        assert!(parse_matchers(r#"{a="b"#).is_err());
        assert!(parse_matchers(r#"{a<"b"}"#).is_err());
        let unquoted = parse_matchers("a=b, c=~d.*").unwrap();
        assert!(unquoted[0].matches("b") && unquoted[1].matches("dz"));
        let escaped = parse_matchers(r#"{a="x\"y"}"#).unwrap();
        assert!(escaped[0].matches("x\"y"));
    }

    #[test]
    fn parses_the_custom_trackers_flag() {
        let pairs = CustomTrackers::parse_flag(r#"a/b:{x="1"};c:{__name__=~"y|z"}"#).unwrap();
        assert_eq!(pairs[0], ("a/b".into(), r#"{x="1"}"#.into()));
        assert_eq!(pairs.len(), 2);
        assert!(CustomTrackers::parse_flag("nocolon").is_err());
    }

    #[test]
    fn indexed_and_unindexed_trackers_match_like_a_full_scan() {
        let sources = [
            ("eq", r#"{job="api"}"#),
            ("alternation", r#"{__name__=~"up|down"}"#),
            ("regex", r#"{__name__=~"up.*"}"#),
            ("negative", r#"{job!="api"}"#),
            ("empty", r#"{missing=""}"#),
            ("two", r#"{job="api", __name__="up"}"#),
        ];
        let trackers = CustomTrackers::new(
            sources
                .iter()
                .map(|(name, matcher)| (name.to_string(), matcher.to_string()))
                .collect(),
        )
        .unwrap();
        for series in [
            labels(&[("__name__", "up"), ("job", "api")]),
            labels(&[("__name__", "down"), ("job", "web")]),
            labels(&[("__name__", "upper"), ("missing", "x")]),
        ] {
            let expected = (0..trackers.len() as u16)
                .filter(|index| {
                    trackers.matchers[usize::from(*index)]
                        .iter()
                        .all(|matcher| matcher.matches(label_value(&series, &matcher.name)))
                })
                .collect::<Vec<_>>();
            assert_eq!(trackers.matching(&series), expected, "{series:?}");
        }
        assert_eq!(
            trackers
                .matching(&labels(&[("__name__", "up"), ("job", "api")]))
                .iter()
                .map(|index| trackers.names()[usize::from(*index)].as_str())
                .collect::<Vec<_>>(),
            ["alternation", "empty", "eq", "regex", "two"]
        );
    }

    #[test]
    fn merges_additional_trackers_over_base() {
        let base = CustomTrackers::new(BTreeMap::from([
            ("a".into(), r#"{x="1"}"#.into()),
            ("b".into(), r#"{x="2"}"#.into()),
        ]))
        .unwrap();
        let extra =
            CustomTrackers::new(BTreeMap::from([("b".into(), r#"{x="3"}"#.into())])).unwrap();
        let merged = base.merged(&extra);
        assert_eq!(merged.names(), ["a", "b"]);
        assert_eq!(merged.matching(&labels(&[("x", "3")])), [1]);
    }

    #[test]
    fn parses_cost_attribution_trackers() {
        let trackers = CostAttributionTrackers::from_value(
            &serde_json::from_str(
                r#"{"source-reservation": {"internal": true, "labels": [
                    {"input": "__grafana_meta_source__", "output": "source"},
                    {"input": "team"}]}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let tracker = &trackers.trackers[0];
        assert!(tracker.internal);
        assert_eq!(tracker.labels[1].output, "team");
        assert_eq!(
            tracker.key(&labels(&[("__grafana_meta_source__", "k6")])),
            ["k6", MISSING_VALUE]
        );
    }
}
