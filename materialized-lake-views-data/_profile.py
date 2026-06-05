import pandas as pd, pickle
from pathlib import Path
HERE = Path(__file__).parent
with open(HERE / '_tables.pkl', 'rb') as f: T = pickle.load(f)
orders, sales, location, agent, ac = T['orders'], T['sales'], T['location'], T['agent'], T['agent_commissions']

def pkcheck(name, df, cols):
    n = len(df); u = df[cols].drop_duplicates().shape[0]
    print(f'  {name}: rows={n}, unique={u}, dup={n-u}')

print('=== Candidate keys (combined snapshots) ===')
pkcheck('orders.Order_ID', orders, ['Order_ID'])
pkcheck('sales.Order_ID',  sales,  ['Order_ID'])
pkcheck('sales.(Order_ID,Agent_ID)', sales, ['Order_ID','Agent_ID'])
pkcheck('location.Order_ID', location, ['Order_ID'])
pkcheck('location.Location_ID', location, ['Location_ID'])
pkcheck('agent.Agent_ID', agent, ['Agent_ID'])
pkcheck('agent_commissions.(Agent_ID,Product_Category)', ac, ['Agent_ID','Product_Category'])

print()
print('=== Single-snapshot key check (first file only) ===')
import glob, os
def first(folder):
    f = sorted(glob.glob(os.path.join(str(HERE), folder, '*.csv')))[0]
    return pd.read_csv(f, dtype=str)
for t, cols in [('orders',['Order_ID']),('sales',['Order_ID']),('location',['Order_ID']),
                ('agent',['Agent_ID']),('agent_commissions',['Agent_ID','Product_Category'])]:
    df = first(t)
    n = len(df); u = df[cols].drop_duplicates().shape[0]
    print(f'  {t} (snap1) {cols}: rows={n}, unique={u}, dup={n-u}')

print()
print('=== Referential integrity (combined) ===')
def fk(name, child, child_col, parent, parent_col):
    pk = set(parent[parent_col].unique()); ck = set(child[child_col].unique())
    print(f'  {name}: child_uniq={len(ck)}, parent_uniq={len(pk)}, missing_in_parent={len(ck-pk)}')
fk('sales.Order_ID -> orders.Order_ID',    sales,'Order_ID', orders,'Order_ID')
fk('sales.Order_ID -> location.Order_ID',  sales,'Order_ID', location,'Order_ID')
fk('orders.Order_ID -> sales.Order_ID',    orders,'Order_ID', sales,'Order_ID')
fk('location.Order_ID -> orders.Order_ID', location,'Order_ID', orders,'Order_ID')
fk('sales.Agent_ID -> agent.Agent_ID',     sales,'Agent_ID', agent,'Agent_ID')
fk('agent_commissions.Agent_ID -> agent.Agent_ID', ac,'Agent_ID', agent,'Agent_ID')

print()
print('=== Composite (sales+orders) -> agent_commissions on (Agent_ID, Product_Category) ===')
so = sales.merge(orders[['Order_ID','Product_Category']], on='Order_ID', how='left')
print(f'  sales+orders rows: {len(so)}; sales rows with no matching order: {so["Product_Category"].isna().sum()}')
so_keys = so[['Agent_ID','Product_Category']].dropna().drop_duplicates()
ac_keys = ac[['Agent_ID','Product_Category']].drop_duplicates()
miss = so_keys.merge(ac_keys, on=['Agent_ID','Product_Category'], how='left', indicator=True)
print(f'  unique (Agent_ID,Product_Category) used: {len(so_keys)}; missing in agent_commissions: {(miss["_merge"]=="left_only").sum()}')

print()
print('=== Cardinality of sales.Agent_ID ===')
print(f'  unique agents in sales: {sales["Agent_ID"].nunique()} / {agent["Agent_ID"].nunique()} agents total')
print(f'  avg sales rows per agent: {len(sales)/sales["Agent_ID"].nunique():.1f}')
print(f'  avg commission rows per agent: {len(ac)/ac["Agent_ID"].nunique():.1f}')
