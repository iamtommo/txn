# simple monetary transaction engine
handles crediting, debiting, disputes, and chargebacks.

expects an input csv file as first and only argument.

streams csv file instead of loading entire data set,
though this perf gain is hindered by retaining transaction logs in-memory, so memory grows nonetheless.

balance mutation is very explicit, no ledger is kept. no double-entry keeping.

should really have hand-written sample input & output data files for end-to-end tests, but unit and engine tests cover most scenarios.

min compiler version 1.46.0 (2020-08-27) as required by rust-decimal

# flaws
output data is not tested.

only deposits and withdrawals are stored in the transaction log.
need another identifier for transactions as i.e. a dispute contains an id of the transaction we're disputing,
but the dispute itself is also a transaction.

currency over/underflows not checked

currency precision truncation is a bit dirty (see Txn#truncate_amount)

could use enums for transaction type permutations

resolve() & chargeback() naively (and dangerously) expect a transaction to exist if it was disputed