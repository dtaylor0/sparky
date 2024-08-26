use std::error::Error;
use std::io::{BufRead, BufReader};
use std::{fmt, fs};

const DISPLAY_COLUMN_WIDTH: usize = 20;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum CellType {
    Integer(i32),
    Float(f64),
    Text(String),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub struct Row {
    pub cells: Vec<CellType>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Column {
    pub data_type: ColumnType,
    pub name: String,
    pub index: usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DataFrame {
    rows: Vec<Row>,
    pub count: usize,
    pub cols: usize,
    pub columns: Vec<Column>,
}

impl ToString for ColumnType {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl ToString for CellType {
    fn to_string(&self) -> String {
        match self {
            CellType::Text(s) => s.to_string(),
            CellType::Float(f) => f.to_string(),
            CellType::Integer(i) => i.to_string(),
            CellType::Boolean(b) => b.to_string(),
        }
    }
}

impl ToString for Row {
    fn to_string(&self) -> String {
        let width = DISPLAY_COLUMN_WIDTH;
        self.cells
            .iter()
            .map(|c| match c {
                CellType::Text(s) => {
                    if s.len() > DISPLAY_COLUMN_WIDTH {
                        let mut s = s.clone();
                        s.truncate(DISPLAY_COLUMN_WIDTH - 3);
                        s.push_str("...");
                        return s;
                    } else {
                        format!("{:<width$}", s)[..width].to_string()
                    }
                }
                CellType::Float(f) => {
                    format!("{:>width$.2}", f)
                }
                CellType::Integer(i) => {
                    format!("{:>width$}", i.to_string())
                }
                CellType::Boolean(b) => {
                    format!("{:>width$}", b.to_string())
                }
            })
            .collect::<Vec<String>>()
            .join("|")
    }
}

pub struct CSVFile(pub String);

impl From<CSVFile> for DataFrame {
    fn from(file: CSVFile) -> Self {
        // Read lines from csv
        let file = fs::File::open(file.0).unwrap();
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // Parse header names
        let header = lines.next().expect("No header found").unwrap();
        let header: Vec<&str> = header.split(",").collect();

        // Parse first row of data, trying to convert to float where possible
        let first = lines.next().expect("No data found").unwrap();
        let first: Vec<CellType> = first
            .split(",")
            .map(|cell| {
                if let Ok(fl) = cell.trim().parse() {
                    CellType::Float(fl)
                } else {
                    CellType::Text(cell.to_string())
                }
            })
            .collect();

        // Get column types from inferred cell types in first row
        let col_types: Vec<Column> = first
            .iter()
            .map(|c| match c {
                CellType::Text(_) => ColumnType::Text,
                CellType::Float(_) => ColumnType::Float,
                CellType::Integer(_) => ColumnType::Integer,
                CellType::Boolean(_) => ColumnType::Boolean,
            })
            .zip(header)
            .enumerate()
            .map(|(i, (data_type, name))| Column {
                data_type,
                name: String::from(name),
                index: i,
            })
            .collect();
        let mut res = DataFrame::new(Some(vec![Row::new(first)]), col_types);

        // Read the remaining rows and add to DataFrame
        for line in lines {
            let line = line.expect("Could not parse a row");
            let mut row = res.rows[0].clone();
            let row = Row::new(
                row.cells
                    .iter_mut()
                    .zip(line.split(","))
                    .map(|(cell, value)| match cell {
                        CellType::Text(_) => CellType::Text(value.to_string()),
                        CellType::Float(_) => {
                            CellType::Float(value.parse().expect("Data is not a float."))
                        }
                        CellType::Integer(_) => {
                            CellType::Integer(value.parse().expect("Data is not an integer."))
                        }
                        CellType::Boolean(_) => {
                            CellType::Boolean(value.parse().expect("Data is not a boolean."))
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            res.rows.push(row);
        }
        res
    }
}

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body: String = self
            .rows
            .iter()
            .map(|r| format!("{}\n", r.to_string()))
            .collect();

        let header: String = self
            .columns
            .iter()
            .map(|c| {
                format!(
                    "{: <width$}",
                    format!("{} [{}]", c.name.to_string(), c.data_type.to_string()),
                    width = DISPLAY_COLUMN_WIDTH
                )
            })
            .collect::<Vec<String>>()
            .join("|");
        let separator = "-".repeat(body.lines().next().unwrap_or("").len());

        write!(f, "{}\n{}\n{}{}", header, separator, body, separator)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum SQLError {
    EmptyDataFrame,
    MismatchedColumns,
}

impl fmt::Display for SQLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SQLError::EmptyDataFrame => write!(f, "One or more dataframes is empty."),
            SQLError::MismatchedColumns => write!(f, "Columns do not match."),
        }
    }
}

impl Error for SQLError {}

impl Row {
    pub fn new(cells: Vec<CellType>) -> Row {
        Row { cells }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Joins {
    LeftInner,
    RightInner,
    LeftOuter,
    RightOuter,
    Full,
}

impl DataFrame {
    pub fn columns_match(&self, other: &DataFrame) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(other.columns.iter())
                .all(|(l, r)| *l == *r)
    }
    pub fn new(rows: Option<Vec<Row>>, columns: Vec<Column>) -> DataFrame {
        let rows = match rows {
            Some(rs) => rs,
            None => vec![],
        };
        let cols = rows.len();
        let count = if cols > 0 { rows[0].cells.len() } else { 0 };
        return DataFrame {
            cols,
            count,
            rows,
            columns,
        };
    }

    pub fn union_all(&mut self, other: &mut DataFrame) -> Result<&DataFrame, Box<dyn Error>> {
        // Validate columns.
        if !self.columns_match(other) {
            return Err(Box::new(SQLError::MismatchedColumns));
        }
        self.rows.append(&mut other.rows);
        self.count += other.count;
        Ok(self)
    }

    /// Joins with another dataframe and returns a mutated dataframe.
    /// # Examples
    ///
    /// ```
    /// use crate::dataframe::Joins
    /// if let Ok(df_joined) = df_left.join(df_right, "ColumnName", Joins::LeftInner) { ... }
    /// ```
    pub fn join(
        &mut self,
        right: &mut DataFrame,
        on: &str,
        join: Joins,
    ) -> Result<&DataFrame, Box<dyn Error>> {
        // Find and validate columns to join on
        let left_col = self.columns.iter().find(|&c| c.name == on);
        let right_col = right.columns.iter().find(|&c| c.name == on);
        let (left_col, right_col) = match (left_col, right_col) {
            (Some(l), Some(r)) => {
                if l.name == r.name {
                    Ok((l, r))
                } else {
                    Err("Mismatched Columns")
                }
            }
            _ => Err("Womp Womp"),
        }?;

        // Route join to helper functions
        match join {
            j => println!(
                "Performing {:?} join on left.{} = right.{}",
                j, left_col.name, right_col.name
            ),
        }

        Ok(self)
    }
}
