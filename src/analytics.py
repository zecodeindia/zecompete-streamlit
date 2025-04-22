# src/analytics.py  – vector‑powered Q&A layer (Colab‐verified pattern)
import pinecone
from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models    import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains         import create_retrieval_chain
from src.config               import secret

# ── 1.  Initialize Pinecone against your index’s own host ───────────
pinecone.init(
    api_key=secret("PINECONE_API_KEY"),
    host="zecompete-1df1x61.svc.aped-4627-b74a.pinecone.io"
)

# ── 2.  Embedding function ───────────────────────────────────────────
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# ── 3.  Bind to your existing index, per namespace ───────────────────
places_vs = PineconeVectorStore.from_existing_index(
    index_name="zecompete",
    embedding=embedding,
    namespace="maps"
)
kw_vs = PineconeVectorStore.from_existing_index(
    index_name="zecompete",
    embedding=embedding,
    namespace="keywords"
)

# ── 4.  LLM for reasoning ─────────────────────────────────────────────
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

# ── 5.  Helper to ask questions ──────────────────────────────────────
def insight_question(question: str) -> str:
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]
