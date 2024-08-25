use crate::dataframe::CellType;
use crate::dataframe::ColumnType;
use crate::dataframe::DataFrame;
use crate::dataframe::Row;

mod dataframe;

fn main() {
    let b = ColumnType::Float == ColumnType::Text;
    println!("Checking: {:?}", b);
    let _sample: Vec<CellType> = vec![1.2, 2.3, 3.4]
        .iter()
        .map(|f| CellType::Float(*f))
        .collect();
    let rows = Some(vec![
        Row::new(vec![
            CellType::Float(10109876543.01234),
            CellType::Float(4.0),
            CellType::Text(String::from("Hello")),
        ]),
        Row::new(vec![
            CellType::Float(0.0),
            CellType::Float(1.0),
            CellType::Text(String::from("Goodbye")),
        ]),
    ]);

    let columns = vec![ColumnType::Float, ColumnType::Float, ColumnType::Text];
    let mut df1: DataFrame = DataFrame::new(rows, columns);
    println!("{}\n\n", df1);

    let rows = Some(vec![
        Row::new(vec![
            CellType::Float(4.1),
            CellType::Float(5.1),
            CellType::Float(6.1),
        ]),
        Row::new(vec![
            CellType::Float(0.1),
            CellType::Float(1.1),
            CellType::Float(2.1),
        ]),
    ]);
    let columns = vec![ColumnType::Float, ColumnType::Float, ColumnType::Float];
    let mut df2: DataFrame = DataFrame::new(rows, columns);
    println!("{}\n\n", df2);

    let rows = Some(vec![
        Row::new(
            vec!["Hello", "There", "."]
                .into_iter()
                .map(|v| CellType::Text(String::from(v)))
                .collect(),
        ),
        Row::new(
            vec!["General", "Kenobi!", "New"]
                .into_iter()
                .map(|v| CellType::Text(String::from(v)))
                .collect(),
        ),
    ]);
    let columns = vec![ColumnType::Text, ColumnType::Text, ColumnType::Text];
    let df3: DataFrame = DataFrame::new(rows, columns);
    println!("{}\n\n", df3);

    let df_union_all = df1.union_all(&mut df2);
    match df_union_all {
        Ok(df) => println!("{}\n\n", df),
        Err(e) => println!("Error occurred: {e}"),
    }
}
