import time
import krishiv as ks

def main():
    print("\n--- Running Complex Batch Query (TPC-H Q1) ---")
    session = ks.Session.from_env()
    session.register_parquet("lineitem", "/home/code/krishiv/tpch_sf10/lineitem.parquet")
    
    q1 = """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    """
    
    import sys
    sys.stdout.flush()
    try:
        start = time.time()
        print("calling collect", flush=True)
        result = session.sql(q1).collect()
        end = time.time()
        print("collect finished", flush=True)
        
        print(result.pretty())
        print(f"Batch Execution Time: {end - start:.4f} seconds")
    except Exception as e:
        print(f"Error: {e}")
    print(f"Batch Execution Time: {end - start:.4f} seconds")

if __name__ == "__main__":
    main()
