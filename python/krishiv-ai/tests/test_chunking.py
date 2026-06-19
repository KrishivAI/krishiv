import krishiv.ai as ks

def test_recursive_chunker():
    c = ks.RecursiveTextChunker(chunk_size=20, overlap=4)
    chunks = c.chunk("one two three four five six seven")
    assert len(chunks) >= 1

def test_sentence_chunker():
    c = ks.SentenceChunker(max_sentences=2)
    chunks = c.chunk("First. Second! Third?")
    assert chunks

def test_markdown_chunker():
    c = ks.MarkdownSectionChunker(min_level=2)
    md = "## A\n\nBody.\n\n## B\n\nMore."
    assert c.chunk(md)
