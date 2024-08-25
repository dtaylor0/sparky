use std::error::Error;
use std::fmt;

#[allow(dead_code)]
#[derive(Debug)]
pub enum CellType {
    Integer(i32),
    Float(f64),
    Text(String),
    Boolean(bool),
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct DataFrame {
    rows: Vec<Row>,
    count: usize,
    cols: usize,
    columns: Vec<ColumnType>,
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
        self.cells
            .iter()
            .map(|c| {
                let mut res = format!("{:>13}", c.to_string());
                res.truncate(13);
                res
            })
            .collect::<Vec<String>>()
            .join("|")
    }
}

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn format_row<T>(row: &Vec<T>) -> String
        where
            T: ToString,
        {
            let row: String = row
                .iter()
                .map(|r| {
                    let mut res = format!("{: >13}", r.to_string());
                    res.truncate(13);
                    res
                })
                .collect::<Vec<String>>()
                .join("|");
            row
        }
        let body: String = self
            .rows
            .iter()
            .map(|r| format!("{}\n", r.to_string()))
            .collect();

        let header = format_row(&self.columns);
        let separator = "-".repeat(body.lines().next().unwrap_or("").len());

        write!(f, "{}\n{}\n{}{}", header, separator, body, separator)
    }
}

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

impl DataFrame {
    pub fn new(rows: Option<Vec<Row>>, columns: Vec<ColumnType>) -> DataFrame {
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

    pub fn union_all(&mut self, right: &mut DataFrame) -> Result<&DataFrame, Box<dyn Error>> {
        // Validate columns and rows.
        if self.cols == 0 || right.cols == 0 {
            return Err(Box::new(SQLError::EmptyDataFrame));
        } else if self.count != right.count {
            return Err(Box::new(SQLError::MismatchedColumns));
        } else if !self
            .columns
            .iter()
            .zip(right.columns.iter())
            .all(|(l, r)| *l == *r)
        {
            return Err(Box::new(SQLError::MismatchedColumns));
        }
        self.rows.append(&mut right.rows);
        self.cols = self.cols + right.cols;
        Ok(self)
    }
}
