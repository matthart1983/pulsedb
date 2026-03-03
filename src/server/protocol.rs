use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

use crate::model::{DataPoint, FieldValue, Tags};

/// Parse a single line of InfluxDB line protocol into a [`DataPoint`].
pub fn parse_line(line: &str) -> Result<DataPoint> {
    let line = line.trim();
    if line.is_empty() {
        bail!("empty line");
    }
    if line.starts_with('#') {
        bail!("comment line");
    }

    let bytes = line.as_bytes();
    let len = bytes.len();

    // --- Measurement: scan until first unescaped ',' or ' ' ---
    let mut i = 0;
    while i < len && bytes[i] != b',' && bytes[i] != b' ' {
        i += 1;
    }
    if i == 0 {
        bail!("missing measurement");
    }
    let measurement = line[..i].to_string();

    // --- Tags (optional): present if we stopped at ',' ---
    let mut tags = Tags::new();
    if i < len && bytes[i] == b',' {
        i += 1; // skip ','
        let tag_start = i;
        // Scan until the first unquoted space.
        while i < len && bytes[i] != b' ' {
            i += 1;
        }
        let tag_str = &line[tag_start..i];
        for pair in tag_str.split(',') {
            let eq = pair
                .find('=')
                .with_context(|| format!("invalid tag pair: {pair}"))?;
            let key = &pair[..eq];
            let val = &pair[eq + 1..];
            if key.is_empty() {
                bail!("empty tag key");
            }
            tags.insert(key.to_string(), val.to_string());
        }
    }

    // We must now be at a space separating measurement/tags from fields.
    if i >= len || bytes[i] != b' ' {
        bail!("missing fields");
    }
    i += 1; // skip space

    // --- Fields (required) ---
    // Find the boundary between fields and the optional timestamp.
    // The timestamp is separated by a space, but strings can contain spaces,
    // so we need to track quoting.
    let fields_start = i;
    let mut in_quotes = false;
    let mut fields_end = len;
    let mut timestamp_start: Option<usize> = None;

    while i < len {
        if bytes[i] == b'"' {
            in_quotes = !in_quotes;
        } else if bytes[i] == b' ' && !in_quotes {
            fields_end = i;
            timestamp_start = Some(i + 1);
            break;
        }
        i += 1;
    }

    let fields_str = &line[fields_start..fields_end];
    let fields = parse_fields(fields_str)?;
    if fields.is_empty() {
        bail!("missing fields");
    }

    // --- Timestamp (optional) ---
    let timestamp = match timestamp_start {
        Some(ts) => {
            let ts_str = line[ts..].trim();
            if ts_str.is_empty() {
                0
            } else {
                ts_str
                    .parse::<i64>()
                    .with_context(|| format!("invalid timestamp: {ts_str}"))?
            }
        }
        None => 0,
    };

    Ok(DataPoint {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Parse multiple lines of InfluxDB line protocol.
///
/// Skips empty lines and comments (lines starting with `#`).
/// Returns an error if any non-skippable line fails to parse.
pub fn parse_lines(input: &str) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let mut errors = Vec::new();

    for (line_num, line) in input.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match parse_line(trimmed) {
            Ok(point) => points.push(point),
            Err(e) => errors.push(format!("line {}: {e}", line_num + 1)),
        }
    }

    if !errors.is_empty() {
        bail!("parse errors:\n{}", errors.join("\n"));
    }

    Ok(points)
}

/// Parse the comma-separated field set, handling quoted strings that may
/// contain commas.
fn parse_fields(fields_str: &str) -> Result<BTreeMap<String, FieldValue>> {
    let mut fields = BTreeMap::new();
    let pairs = split_fields(fields_str);

    for pair in pairs {
        let eq = pair
            .find('=')
            .with_context(|| format!("invalid field pair: {pair}"))?;
        let key = &pair[..eq];
        let raw_val = &pair[eq + 1..];
        if key.is_empty() {
            bail!("empty field key");
        }
        let value = parse_field_value(raw_val)
            .with_context(|| format!("invalid value for field '{key}': {raw_val}"))?;
        fields.insert(key.to_string(), value);
    }

    Ok(fields)
}

/// Split field pairs by commas, respecting double-quoted strings.
fn split_fields(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut in_quotes = false;

    for i in 0..bytes.len() {
        if bytes[i] == b'"' {
            in_quotes = !in_quotes;
        } else if bytes[i] == b',' && !in_quotes {
            parts.push(&s[start..i]);
            start = i + 1;
        }
    }
    if start <= s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Parse a single field value string into a [`FieldValue`].
fn parse_field_value(raw: &str) -> Result<FieldValue> {
    if raw.is_empty() {
        bail!("empty field value");
    }

    // String value: starts and ends with '"'
    if raw.starts_with('"') {
        if raw.len() < 2 || !raw.ends_with('"') {
            bail!("unterminated string value");
        }
        return Ok(FieldValue::String(raw[1..raw.len() - 1].to_string()));
    }

    // Boolean
    match raw {
        "t" | "T" | "true" | "TRUE" => return Ok(FieldValue::Boolean(true)),
        "f" | "F" | "false" | "FALSE" => return Ok(FieldValue::Boolean(false)),
        _ => {}
    }

    // Integer (i suffix)
    if raw.ends_with('i') {
        let num_str = &raw[..raw.len() - 1];
        let v = num_str
            .parse::<i64>()
            .with_context(|| format!("invalid integer: {num_str}"))?;
        return Ok(FieldValue::Integer(v));
    }

    // Unsigned integer (u suffix)
    if raw.ends_with('u') {
        let num_str = &raw[..raw.len() - 1];
        let v = num_str
            .parse::<u64>()
            .with_context(|| format!("invalid unsigned integer: {num_str}"))?;
        return Ok(FieldValue::UInteger(v));
    }

    // Float (default numeric)
    let v = raw
        .parse::<f64>()
        .with_context(|| format!("invalid float: {raw}"))?;
    Ok(FieldValue::Float(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_line() {
        let p = parse_line("cpu,host=server01 value=1.0 1609459200000000000").unwrap();
        assert_eq!(p.measurement, "cpu");
        assert_eq!(p.tags.get("host").unwrap(), "server01");
        assert_eq!(p.fields.get("value").unwrap(), &FieldValue::Float(1.0));
        assert_eq!(p.timestamp, 1609459200000000000);
    }

    #[test]
    fn test_multiple_tags_and_fields() {
        let p = parse_line(
            "weather,city=nyc,station=central temp=72.5,humidity=45i 1609459200000000000",
        )
        .unwrap();
        assert_eq!(p.measurement, "weather");
        assert_eq!(p.tags.len(), 2);
        assert_eq!(p.tags.get("city").unwrap(), "nyc");
        assert_eq!(p.tags.get("station").unwrap(), "central");
        assert_eq!(p.fields.get("temp").unwrap(), &FieldValue::Float(72.5));
        assert_eq!(
            p.fields.get("humidity").unwrap(),
            &FieldValue::Integer(45)
        );
        assert_eq!(p.timestamp, 1609459200000000000);
    }

    #[test]
    fn test_all_field_types() {
        let p = parse_line(
            r#"m,t=v flt=3.14,int=42i,uint=100u,b=true,s="hello" 1000"#,
        )
        .unwrap();
        assert_eq!(p.fields.get("flt").unwrap(), &FieldValue::Float(3.14));
        assert_eq!(p.fields.get("int").unwrap(), &FieldValue::Integer(42));
        assert_eq!(p.fields.get("uint").unwrap(), &FieldValue::UInteger(100));
        assert_eq!(p.fields.get("b").unwrap(), &FieldValue::Boolean(true));
        assert_eq!(
            p.fields.get("s").unwrap(),
            &FieldValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_missing_timestamp() {
        let p = parse_line("cpu,host=a value=1.0").unwrap();
        assert_eq!(p.timestamp, 0);
    }

    #[test]
    fn test_no_tags() {
        let p = parse_line("cpu value=42.0 1000").unwrap();
        assert_eq!(p.measurement, "cpu");
        assert!(p.tags.is_empty());
        assert_eq!(p.fields.get("value").unwrap(), &FieldValue::Float(42.0));
        assert_eq!(p.timestamp, 1000);
    }

    #[test]
    fn test_float_without_decimal() {
        let p = parse_line("m value=42").unwrap();
        assert_eq!(p.fields.get("value").unwrap(), &FieldValue::Float(42.0));
    }

    #[test]
    fn test_integer_suffix() {
        let p = parse_line("m count=42i").unwrap();
        assert_eq!(p.fields.get("count").unwrap(), &FieldValue::Integer(42));
    }

    #[test]
    fn test_unsigned_suffix() {
        let p = parse_line("m count=42u").unwrap();
        assert_eq!(p.fields.get("count").unwrap(), &FieldValue::UInteger(42));
    }

    #[test]
    fn test_boolean_variations() {
        for (input, expected) in [
            ("t", true),
            ("T", true),
            ("true", true),
            ("TRUE", true),
            ("f", false),
            ("F", false),
            ("false", false),
            ("FALSE", false),
        ] {
            let line = format!("m b={input}");
            let p = parse_line(&line).unwrap();
            assert_eq!(
                p.fields.get("b").unwrap(),
                &FieldValue::Boolean(expected),
                "failed for input: {input}"
            );
        }
    }

    #[test]
    fn test_string_field_with_spaces() {
        let p = parse_line(r#"m msg="hello world""#).unwrap();
        assert_eq!(
            p.fields.get("msg").unwrap(),
            &FieldValue::String("hello world".to_string())
        );
    }

    #[test]
    fn test_string_field_with_spaces_and_timestamp() {
        let p = parse_line(r#"m msg="hello world" 1000"#).unwrap();
        assert_eq!(
            p.fields.get("msg").unwrap(),
            &FieldValue::String("hello world".to_string())
        );
        assert_eq!(p.timestamp, 1000);
    }

    #[test]
    fn test_comment_line() {
        assert!(parse_line("# this is a comment").is_err());
    }

    #[test]
    fn test_empty_line() {
        assert!(parse_line("").is_err());
    }

    #[test]
    fn test_batch_parsing() {
        let input = "\
cpu,host=a value=1.0 1000
cpu,host=b value=2.0 2000
cpu,host=c value=3.0 3000
";
        let points = parse_lines(input).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].tags.get("host").unwrap(), "a");
        assert_eq!(points[1].tags.get("host").unwrap(), "b");
        assert_eq!(points[2].tags.get("host").unwrap(), "c");
    }

    #[test]
    fn test_batch_skips_comments_and_empty() {
        let input = "\
# header comment
cpu value=1.0 1000

# another comment

mem value=2.0 2000
";
        let points = parse_lines(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].measurement, "cpu");
        assert_eq!(points[1].measurement, "mem");
    }

    #[test]
    fn test_missing_measurement() {
        assert!(parse_line(" value=1.0").is_err());
    }

    #[test]
    fn test_missing_fields() {
        assert!(parse_line("cpu").is_err());
    }

    #[test]
    fn test_malformed_field_value() {
        assert!(parse_line("cpu value=notanumber_xyz").is_err());
    }

    #[test]
    fn test_negative_float() {
        let p = parse_line("m value=-42.5").unwrap();
        assert_eq!(p.fields.get("value").unwrap(), &FieldValue::Float(-42.5));
    }

    #[test]
    fn test_negative_integer() {
        let p = parse_line("m count=-10i").unwrap();
        assert_eq!(p.fields.get("count").unwrap(), &FieldValue::Integer(-10));
    }

    #[test]
    fn test_scientific_notation() {
        let p = parse_line("m value=1.5e10").unwrap();
        assert_eq!(
            p.fields.get("value").unwrap(),
            &FieldValue::Float(1.5e10)
        );
    }

    #[test]
    fn test_large_timestamp() {
        let p = parse_line("m value=1.0 1709459200000000000").unwrap();
        assert_eq!(p.timestamp, 1709459200000000000);
    }

    #[test]
    fn test_batch_with_error() {
        let input = "\
cpu value=1.0 1000
bad_line_no_fields
mem value=2.0 2000
";
        let result = parse_lines(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_tags_no_timestamp() {
        let p = parse_line("cpu value=1.0").unwrap();
        assert_eq!(p.measurement, "cpu");
        assert!(p.tags.is_empty());
        assert_eq!(p.fields.get("value").unwrap(), &FieldValue::Float(1.0));
        assert_eq!(p.timestamp, 0);
    }

    #[test]
    fn test_multiple_fields_no_tags() {
        let p = parse_line("cpu temp=98.6,load=0.75 5000").unwrap();
        assert_eq!(p.measurement, "cpu");
        assert!(p.tags.is_empty());
        assert_eq!(p.fields.len(), 2);
        assert_eq!(p.fields.get("temp").unwrap(), &FieldValue::Float(98.6));
        assert_eq!(p.fields.get("load").unwrap(), &FieldValue::Float(0.75));
        assert_eq!(p.timestamp, 5000);
    }
}
