use crate::errors;
use std::collections::VecDeque;

/// Splits an SQL statement into tokens, respecting spaces inside parentheses and quotes.
///
/// # Arguments
/// * `sql` - The raw SQL string to split.
///
/// # Returns
/// A `Result` containing a `VecDeque<String>` of tokens or an `errors::Error` if syntax is invalid.
pub fn tokenize_sql(sql: &str) -> Result<VecDeque<String>, errors::Error> {
    let mut result = VecDeque::new();
    let mut current = String::new();
    let mut inside_parens = false;
    let mut inside_text = false;

    for c in sql.chars() {
        match c {
            '\'' => {
                inside_text = !inside_text;
                current.push(c);
            }
            ')' => {
                if !inside_text {
                    if !inside_parens {
                        return Err(errors::Error::Syntax(
                            "Unmatched closing parenthesis.".to_owned(),
                        ));
                    }
                    inside_parens = false;
                }
                current.push(c);
            }
            '(' => {
                if !inside_text {
                    if inside_parens {
                        return Err(errors::Error::Syntax(
                            "Nested opening parenthesis.".to_owned(),
                        ));
                    }
                    inside_parens = true;
                }
                current.push(c);
            }
            ' ' if !inside_parens && !inside_text => {
                if !current.is_empty() {
                    result.push_back(current);
                    current = String::new();
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        result.push_back(current);
    }
    if inside_parens {
        return Err(errors::Error::Syntax(
            "Missing closing parenthesis.".to_owned(),
        ));
    }
    if inside_text {
        return Err(errors::Error::Syntax("Unclosed text literal.".to_owned()));
    }

    Ok(result)
}
