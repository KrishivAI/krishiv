import os, krishiv as ks
session = ks.Session.from_env()
result = session.sql("SELECT 1 as baremetal_test").collect()
print(result.pretty())
