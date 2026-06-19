//! Prepared SQL with typed positional bind parameters.

use krishiv_plan::expression::ScalarValue;

use crate::{DataFrame, KrishivError, Result, Session};

#[derive(Clone)]
pub struct PreparedStatement {
    session: Session,
    sql: String,
    parameter_count: usize,
}

impl PreparedStatement {
    pub(crate) fn new(session: Session, sql: String) -> Result<Self> {
        let parameter_count = validate_placeholders(&sql)?;
        Ok(Self {
            session,
            sql,
            parameter_count,
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    pub fn bind(&self, parameters: &[ScalarValue]) -> Result<DataFrame> {
        if parameters.len() != self.parameter_count {
            return Err(KrishivError::InvalidConfig {
                message: format!(
                    "prepared statement expects {} parameters, got {}",
                    self.parameter_count,
                    parameters.len()
                ),
            });
        }
        self.session.sql(bind_parameters(&self.sql, parameters)?)
    }
}

impl Session {
    pub fn prepare(&self, sql: impl Into<String>) -> Result<PreparedStatement> {
        PreparedStatement::new(self.clone(), sql.into())
    }
}

fn validate_placeholders(sql: &str) -> Result<usize> {
    let mut maximum = 0usize;
    scan_sql(sql, |index| maximum = maximum.max(index))?;
    Ok(maximum)
}

fn bind_parameters(sql: &str, parameters: &[ScalarValue]) -> Result<String> {
    let mut output = String::with_capacity(sql.len() + parameters.len() * 8);
    let mut last = 0usize;
    scan_sql_with_offsets(sql, |start, end, index| {
        output.push_str(&sql[last..start]);
        output.push_str(&parameters[index - 1].to_sql_literal());
        last = end;
    })?;
    output.push_str(&sql[last..]);
    Ok(output)
}

fn scan_sql(sql: &str, mut visit: impl FnMut(usize)) -> Result<()> {
    scan_sql_with_offsets(sql, |_, _, index| visit(index))
}

fn scan_sql_with_offsets(sql: &str, mut visit: impl FnMut(usize, usize, usize)) -> Result<()> {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut quote = None;
    let mut line_comment = false;
    let mut block_comment = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if line_comment {
            if byte == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(index + 1) == Some(&active) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            line_comment = true;
            index += 2;
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            block_comment = true;
            index += 2;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            index += 1;
            continue;
        }
        if byte == b'$' && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
            let start = index;
            index += 1;
            let digits = index;
            while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
            let parameter = sql[digits..index].parse::<usize>().map_err(|error| {
                KrishivError::InvalidConfig {
                    message: format!("invalid SQL parameter: {error}"),
                }
            })?;
            if parameter == 0 {
                return Err(KrishivError::InvalidConfig {
                    message: "prepared statement parameters are one-based ($1, $2, ...)".into(),
                });
            }
            visit(start, index, parameter);
            continue;
        }
        index += 1;
    }
    if quote.is_some() || block_comment {
        return Err(KrishivError::InvalidConfig {
            message: "unterminated quoted value or block comment in prepared SQL".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_is_typed_and_ignores_quoted_placeholders() {
        let sql = "SELECT $1 AS value, '$2' AS literal -- $3\n/* $4 */";
        assert_eq!(validate_placeholders(sql).unwrap(), 1);
        assert_eq!(
            bind_parameters(sql, &[ScalarValue::Utf8("O'Reilly".into())]).unwrap(),
            "SELECT 'O''Reilly' AS value, '$2' AS literal -- $3\n/* $4 */"
        );
    }
}
