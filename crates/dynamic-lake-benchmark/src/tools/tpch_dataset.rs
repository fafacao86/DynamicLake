//! TPC-H customer and order keys for the OJV suite.

use std::error::Error;

use tpchgen::generators::{CustomerGenerator, OrderGenerator};

pub struct TpchDataset {
    pub customers: Vec<(i64, i64)>,
    pub initial_orders: Vec<(i64, i64)>,
    pub new_orders: Vec<(i64, i64)>,
}

pub fn tpch_dataset(row_count: i64, change_count: usize) -> Result<TpchDataset, Box<dyn Error>> {
    if row_count < 1 || change_count == 0 || change_count > row_count as usize {
        return Err("expected rows > 0 and 0 < changes <= rows".into());
    }
    let scale = (row_count as f64 / 150_000.0).max(0.001);
    let customers = CustomerGenerator::new(scale, 1, 1)
        .iter()
        .take(row_count as usize)
        .map(|row| (row.c_custkey, row.c_custkey))
        .collect::<Vec<_>>();
    if customers.len() != row_count as usize {
        return Err("tpchgen did not produce enough customers".into());
    }
    let mut orders = OrderGenerator::new(scale, 1, 1)
        .iter()
        .filter(|row| row.o_custkey <= row_count)
        .take(row_count as usize + change_count)
        .map(|row| (row.o_orderkey, row.o_custkey));
    let initial_orders = orders.by_ref().take(row_count as usize).collect::<Vec<_>>();
    let new_orders = orders.collect::<Vec<_>>();
    if initial_orders.len() != row_count as usize || new_orders.len() != change_count {
        return Err("tpchgen did not produce enough matching orders".into());
    }
    Ok(TpchDataset {
        customers,
        initial_orders,
        new_orders,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_customer_order_delta() -> Result<(), Box<dyn Error>> {
        let dataset = tpch_dataset(100, 10)?;
        assert_eq!(dataset.customers.len(), 100);
        assert_eq!(dataset.initial_orders.len(), 100);
        assert_eq!(dataset.new_orders.len(), 10);
        assert!(
            dataset
                .new_orders
                .iter()
                .all(|(_, customer)| *customer <= 100)
        );
        Ok(())
    }
}
