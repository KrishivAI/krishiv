import krishiv.ai as ks

def test_rag_index_metrics():
    docs = [("d1", "hello world"), ("d2", "foo bar")]
    total, embedded, skipped = ks.rag_index(docs, epoch=1)
    assert total == 2
    assert embedded + skipped >= 0
