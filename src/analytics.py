# src/analytics.py  – vector‑powered Q&A layer for Serverless AWS

import pinecone
from src.config import secret
from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models  import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains      import create_retrieval_chain

# ── 1) Init Pinecone against your **control‑plane** host -----------
#     (this is the host URL you saw in the Index detail, minus "https://")
pinecone.init(
    api_key=secret("PINECONE_API_KEY"),
    host="controller.us-east-1.aws.pinecone.io"
)

# ── 2) Prepare your embedding object --------------------------------
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# ── 3) Bind to the existing index & namespaces ----------------------
INDEX_NAME = "zecompete"  # must match your index name exactly

places_vs = PineconeVectorStore.from_existing_index(
    index_name=INDEX_NAME,
    embedding=embedding,
    namespace="maps"
)

kw_vs = PineconeVectorStore.from_existing_index(
    index_name=INDEX_NAME,
    embedding=embedding,
    namespace="keywords"
)

# ── 4) LLM setup -----------------------------------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

# ── 5) Exposed helper ------------------------------------------------
def insight_question(question: str) -> str:
    """
    Ask a question grounded in your Pinecone data,
    backed by LangChain Retrieval‑QA.
    """
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]
