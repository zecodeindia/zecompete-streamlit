# analytics.py – vector‑powered Q&A layer
from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models  import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains import create_retrieval_chain
from src.config import secret
import pinecone

# -- 1.  Init Pinecone SDK v2 ------------------------------------------
pinecone.init(
    api_key=secret("PINECONE_API_KEY"),
    environment=secret("PINECONE_ENV")     # "us-east-1-aws"
)

INDEX_NAME = "zecompete"

# -- 2.  Embedding function --------------------------------------------
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# -- 3.  Attach to existing index & namespaces -------------------------
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

# -- 4.  LLM for reasoning --------------------------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

# -- 5.  Public helper --------------------------------------------------
def insight_question(question: str) -> str:
    """Return a narrative answer grounded in Pinecone‑stored place data."""
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]