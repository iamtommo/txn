use std::collections::{HashMap, HashSet};
use std::io::Write;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

const CURRENCY_PRECISION: u32 = 4;

type ClientId = u16;
type Accounts = HashMap<ClientId, Account>;
type TxnId = u32;

#[derive(Debug, Eq, PartialEq, Default)]
struct Account {
    balance: Balance,
    disputes: HashSet<TxnId>,
    txnlog: HashMap<TxnId, Txn>,
    locked: bool
}

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
enum TxnType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback
}

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
struct Txn {
    #[serde(rename = "type")]
    txntype: TxnType,
    client: ClientId,
    tx: TxnId,
    amount: Option<Decimal>
}

#[derive(Debug, Eq, PartialEq, Default, Copy, Clone)]
struct Balance {
    /// total - held
    available: Decimal,
    /// total - available
    held: Decimal,
    /// available + held
    total: Decimal
}

impl Txn {
    fn new(txntype: TxnType, client: ClientId, tx: TxnId, amount: Option<Decimal>) -> Self {
        Self {
            txntype, client, tx,
            amount: amount.map_or(None, |a| Some(a.round_dp(CURRENCY_PRECISION)))
        }
    }

    fn deposit(client: ClientId, tx: TxnId, amount: Decimal) -> Self {
        Txn::new(TxnType::Deposit, client, tx, Some(amount))
    }

    fn withdrawal(client: ClientId, tx: TxnId, amount: Decimal) -> Self {
        Txn::new(TxnType::Withdrawal, client, tx, Some(amount))
    }

    fn dispute(client: ClientId, tx: TxnId) -> Self {
        Txn::new(TxnType::Dispute, client, tx, None)
    }

    fn resolve(client: ClientId, tx: TxnId) -> Self {
        Txn::new(TxnType::Resolve, client, tx, None)
    }

    fn chargeback(client: ClientId, tx: TxnId) -> Self {
        Txn::new(TxnType::Chargeback, client, tx, None)
    }

    fn amount(&self) -> Decimal {
        self.amount.unwrap_or(dec!(0.0))
    }

    fn truncate_amount(&mut self) -> &mut Txn {
        if self.amount.is_none() {
            return self;
        }
        self.amount = Some(self.amount().round_dp(CURRENCY_PRECISION));
        self
    }
}

/// safe. creates if it doesn't exist.
fn get_account_mut(accounts: &mut Accounts, client: ClientId) -> &mut Account {
    return accounts.entry(client).or_insert_with(|| Account::default());
}

/// safe. returns default empty balance if account does not exist.
fn get_balance(accounts: &Accounts, client: ClientId) -> Balance {
    match accounts.get(&client) {
        Some(acc) => acc.balance,
        None => Balance::default()
    }
}

fn deposit(accounts: &mut Accounts, client: ClientId, amount: Decimal) {
    let account = get_account_mut(accounts, client);
    account.balance.available += amount;
    account.balance.total += amount;
}

fn withdraw(accounts: &mut Accounts, client: ClientId, amount: Decimal) {
    let account = get_account_mut(accounts, client);
    if account.balance.available < amount {
        return;
    }

    account.balance.available -= amount;
    account.balance.total -= amount;
}

fn dispute(accounts: &mut Accounts, client: ClientId, tx: TxnId) {
    let account = get_account_mut(accounts, client);
    let txn = match account.txnlog.get(&tx) {
        Some(t) => t,
        None => {
            // nonexistent transaction
            return;
        }
    };

    let newly_disputed = account.disputes.insert(tx);
    if !newly_disputed {
        // do not deduct available
        return;
    }

    account.balance.available -= txn.amount();
    account.balance.held += txn.amount();
}

fn resolve(accounts: &mut Accounts, client: ClientId, tx: TxnId) {
    let account = get_account_mut(accounts, client);
    let removed = account.disputes.remove(&tx);
    if !removed {
        // transaction is not under dispute
        return;
    }

    let txn: &Txn = account.txnlog.get(&tx).unwrap();// dangerous, but fine to assume since txnlogs are never cleared
    account.balance.available += txn.amount();
    account.balance.held -= txn.amount();
}

fn chargeback(accounts: &mut Accounts, client: ClientId, tx: TxnId) {
    let account = get_account_mut(accounts, client);
    let disputed = account.disputes.contains(&tx);
    if !disputed {
        // cannot chargeback an undisputed transaction?
        return;
    }

    let txn: &Txn = account.txnlog.get(&tx).unwrap();// dangerous, but fine to assume since txnlogs are never cleared
    account.balance.held -= txn.amount();
    account.balance.total -= txn.amount();
    account.disputes.remove(&tx);
    lock(accounts, client);
}

fn lock(accounts: &mut Accounts, client: ClientId) {
    get_account_mut(accounts, client).locked = true;
}

fn is_locked(accounts: &Accounts, client: ClientId) -> bool {
    return match accounts.get(&client) {
        Some(acc) => acc.locked,
        None => false
    };
}

fn log_transaction(accounts: &mut Accounts, transaction: Txn) {
    get_account_mut(accounts, transaction.client).txnlog.insert(transaction.tx, transaction);
}

fn execute(accounts: &mut Accounts, txn: Txn) {
    if is_locked(&accounts, txn.client) {
        return;
    }
    match txn.txntype {
        TxnType::Deposit => {
            deposit(accounts, txn.client, txn.amount());
            log_transaction(accounts, txn);
        },
        TxnType::Withdrawal => {
            withdraw(accounts, txn.client, txn.amount());
            log_transaction(accounts, txn);
        },
        TxnType::Dispute => {
            dispute(accounts, txn.client, txn.tx)
        },
        TxnType::Resolve => {
            resolve(accounts, txn.client, txn.tx)
        },
        TxnType::Chargeback => {
            chargeback(accounts, txn.client, txn.tx)
        }
    }
}

/// trims, deserializes & truncates amount
fn deserialize_record(record: &mut csv::StringRecord) -> csv::Result<Txn> {
    record.trim();
    match record.deserialize::<Txn>(Option::None) {
        Ok(mut t) => Ok(t.truncate_amount().clone()),
        Err(e) => Err(e)
    }
}

fn write_out(accounts: &Accounts) {
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    writer.write_record(&["client", "available", "held", "total", "locked"]);
    for (client, account) in accounts.iter() {
        let balance = account.balance;
        writer.serialize((client, balance.available, balance.held, balance.total, account.locked));
    }
    writer.flush();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut accounts = Accounts::new();

    let file_path = match std::env::args_os().nth(1) {
        Some(path) => path,
        None => return Err("Usage: txn <file>".into())
    };

    let reader = match csv::Reader::from_path(file_path) {
        Ok(r) => r,
        Err(_) => return Err("Error reading file".into())
    };

    // use streaming iterator to avoid loading entire dataset
    for row in reader.into_records() {
        let mut d = match row {
            Ok(d) => d,
            Err(_) => return Err("Malformatted row".into())
        };

        let txn = match deserialize_record(&mut d) {
            Ok(t) => t,
            Err(_) => return Err("Malformatted row".into())
        };

        execute(&mut accounts, txn);
    }

    write_out(&accounts);

    Ok(())
}

#[cfg(test)]
mod engine_tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{Accounts, ClientId, deposit, execute, get_account_mut, get_balance, is_locked, lock, Txn, TxnId, withdraw};

    #[test]
    fn test_chargeback() {
        let mut accounts = Accounts::new();
        let client: ClientId = 1;

        // deposit 10 (tx 1), then 2 (tx 2)
        execute(&mut accounts, Txn::deposit(client, 1, dec!(10)));
        execute(&mut accounts, Txn::deposit(client, 2, dec!(2)));
        assert_eq!(get_balance(&accounts, client).available, dec!(12.0));

        // dispute tx 2
        execute(&mut accounts, Txn::dispute(client, 2));
        let balance = get_balance(&accounts, client);
        assert_eq!(balance.available, dec!(10.0));
        assert_eq!(balance.held, dec!(2.0));
        assert_eq!(balance.total, dec!(12.0));

        // chargeback
        execute(&mut accounts, Txn::chargeback(client, 2));
        let balance = get_balance(&accounts, client);
        assert_eq!(is_locked(&accounts, client), true);
        assert_eq!(balance.held, dec!(0));
        assert_eq!(balance.available, dec!(10));
        assert_eq!(balance.total, dec!(10))
    }

    #[test]
    fn test_chargeback_undisputed() {
        let mut accounts = Accounts::new();
        let client: ClientId = 1;

        // start with a total
        execute(&mut accounts, Txn::deposit(client, 1, dec!(10)));
        assert_eq!(get_balance(&accounts, client).total, dec!(10.0));

        // attempt a chargeback & assert nothing happened
        execute(&mut accounts, Txn::chargeback(client, 1));
        assert_eq!(get_balance(&accounts, client).total, dec!(10.0));
    }

    #[test]
    fn test_locked() {
        let mut accounts = Accounts::new();
        let client: ClientId = 1;

        // start with an initial total
        execute(&mut accounts, Txn::deposit(client, 1, dec!(10)));

        // lock the account
        lock(&mut accounts, client);
        assert_eq!(is_locked(&accounts, client), true);

        // assert we can no longer deposit
        execute(&mut accounts, Txn::deposit(client, 2, dec!(2.0)));
        assert_eq!(get_balance(&accounts, client).available, dec!(10.0));

        // & assert we can not withdraw
        execute(&mut accounts, Txn::deposit(client, 3, dec!(1.0)));
        assert_eq!(get_balance(&accounts, client).available, dec!(10.0));
    }

    #[test]
    fn test_dispute_resolve() {
        let mut accounts = Accounts::new();

        // dispute
        let tx: TxnId = 10;
        execute(&mut accounts, Txn::deposit(1, tx, dec!(10.0)));
        execute(&mut accounts, Txn::dispute(1, tx));
        let balance = get_balance(&accounts, 1);
        assert_eq!(balance.available, dec!(0));
        assert_eq!(balance.held, dec!(10.0));
        assert_eq!(balance.total, dec!(10.0));

        // resolve
        execute(&mut accounts, Txn::resolve(1, tx));
        let balance = get_balance(&accounts, 1);
        assert_eq!(balance.available, dec!(10.0));
        assert_eq!(balance.held, dec!(0));
        assert_eq!(balance.total, dec!(10.0));
    }

    #[test]
    fn test_dispute() {
        let mut accounts = Accounts::new();

        // deposit 10 (tx 1), then 2 (tx 2)
        execute(&mut accounts, Txn::deposit(1, 1, dec!(10.0)));
        execute(&mut accounts, Txn::deposit(1, 2, dec!(2.0)));
        assert_eq!(get_balance(&accounts, 1).available, dec!(12.0));

        // dispute tx 1
        // assert available is 2 & held is 10
        execute(&mut accounts, Txn::dispute(1, 1));
        let balance = get_balance(&accounts, 1);
        assert_eq!(balance.available, dec!(2.0));
        assert_eq!(balance.held, dec!(10.0));

        // total must remain as available + held
        assert_eq!(balance.available + balance.held, dec!(12.0));
    }

    #[test]
    fn test_dispute_invalid_transaction() {
        let mut accounts = Accounts::new();
        execute(&mut accounts, Txn::deposit(1, 1, dec!(10.0)));
        assert_eq!(get_balance(&accounts, 1).available, dec!(10.0));

        // dispute an invalid txn id & assert it was ignored
        execute(&mut accounts, Txn::dispute(1, 50));
        assert_eq!(get_balance(&accounts, 1).available, dec!(10.0));
    }

    #[test]
    fn test_deposit_withdraw() {
        let mut accounts = Accounts::new();

        deposit(&mut accounts, 1, dec!(42.0));
        assert_eq!(dec!(42), get_balance(&accounts, 1).available);

        withdraw(&mut accounts, 1, dec!(42.0));
        assert_eq!(dec!(0), get_balance(&accounts, 1).available);
    }

    #[test]
    fn test_withdraw_exceeds_available() {
        let mut accounts = Accounts::new();
        deposit(&mut accounts, 1, dec!(42.0));

        let withdrawal = dec!(0.0001);
        withdraw(&mut accounts, 1, withdrawal);
        let expected = dec!(41.9999);
        assert_eq!(get_balance(&accounts, 1).available, expected);

        withdraw(&mut accounts, 1, dec!(42.0));
        assert_eq!(get_balance(&accounts, 1).available, expected);
    }

    #[test]
    fn test_withdraw_empty_account() {
        let mut accounts = Accounts::new();

        withdraw(&mut accounts, 1, dec!(1));
        assert_eq!(dec!(0), get_balance(&accounts, 1).available);
    }
}

#[cfg(test)]
mod unit_tests {
    use rust_decimal::Decimal;
    use rust_decimal::prelude::FromStr;
    use rust_decimal_macros::dec;

    use crate::{Accounts, ClientId, CURRENCY_PRECISION, deposit, deserialize_record, get_account_mut, get_balance, Txn, TxnId, TxnType};

    #[test]
    fn test_deposit() {
        let mut accounts = Accounts::new();
        deposit(&mut accounts, 1, dec!(3.14));
        let acc = get_balance(&accounts, 1);
        assert_eq!(acc.available, dec!(3.14));
        assert_eq!(acc.total, dec!(3.14));
    }

    #[test]
    fn test_txn_eq() {
        assert_eq!(Txn::withdrawal(1, 2, Decimal::new(1, 0)),
        Txn::withdrawal(1, 2, dec!(1.0)));

        assert_ne!(Txn::withdrawal(1, 2, Decimal::new(1, 0)),
        Txn::withdrawal(1, 2, dec!(1.0001)));
    }

    #[test]
    fn test_decimal_truncate() {
        assert_eq!(dec!(3.14159).round_dp(4), dec!(3.1416));
    }

    #[test]
    fn test_txn_precision() {
        assert_eq!(Txn::withdrawal(1, 2, dec!(1.11111)),
                   Txn::new(TxnType::Withdrawal, 1, 2, Some(dec!(1.1111))));
    }

    #[test]
    fn test_deserialize() {
        let mut record = csv::StringRecord::from(vec!["deposit", "1", "2", "3.1459"]);
        assert_eq!(deserialize_record(&mut record).unwrap(), Txn::deposit(1, 2, dec!(3.1459)));
    }

    #[test]
    fn test_deserialize_missing_amount() {
        let mut record = csv::StringRecord::from(vec!["dispute", "1", "2", ""]);
        assert_eq!(deserialize_record(&mut record).unwrap(), Txn::dispute(1, 2));
    }

    #[test]
    fn test_deserialize_whitespace() {
        let mut record = csv::StringRecord::from(vec!["    withdrawal", " 1", " 2 ", "3   "]);
        assert_eq!(deserialize_record(&mut record).unwrap(), Txn::withdrawal(1, 2, Decimal::from_str("3.0").unwrap()));
    }

    #[test]
    fn test_deserialize_decimal() {
        let mut record = csv::StringRecord::from(vec!["deposit", "1", "2", "3.1459265"]);
        println!("out: {:?}", deserialize_record(&mut record).unwrap());
        assert_eq!(deserialize_record(&mut record).unwrap(), Txn::deposit(1, 2, dec!(3.1459)));
    }

    #[test]
    fn test_deserialize_decimal_precision() {
        let mut record = csv::StringRecord::from(vec!["deposit", "1", "2", "3.1459265"]);
        assert_eq!(deserialize_record(&mut record).unwrap(), Txn::deposit(1, 2, dec!(3.1459)));
    }

    #[test]
    fn test_deserialize_invalid_client_id() {
        let mut underflow = csv::StringRecord::from(vec!["deposit", (ClientId::MIN as i32 - 1).to_string().as_str(), "1", "3.1459265"]);
        let mut overflow = csv::StringRecord::from(vec!["deposit", (ClientId::MAX as i32 + 1).to_string().as_str(), "2", "3.1459265"]);
        assert_eq!(deserialize_record(&mut underflow).is_err(), true);
        assert_eq!(deserialize_record(&mut overflow).is_err(), true);
    }

    #[test]
    fn test_deserialize_invalid_txn_id() {
        let mut underflow = csv::StringRecord::from(vec!["deposit", "1", (TxnId::MIN as i128 - 1).to_string().as_str(), "3.1459265"]);
        let mut overflow = csv::StringRecord::from(vec!["deposit", "1", (TxnId::MAX as i128 + 1).to_string().as_str(), "3.1459265"]);
        assert_eq!(deserialize_record(&mut underflow).is_err(), true);
        assert_eq!(deserialize_record(&mut overflow).is_err(), true);
    }
}