from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models  import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from src.config import secret
import pinecone

# -- 1.  initialise Pinecone SDK v2 -----------------------------------
pinecone.init(
    api_key=secret("PINECONE_API_KEY"),
    environment=secret("PINECONE_ENV")
)

# -- 2.  create ONE embedding object ----------------------------------
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# -- 3.  attach to the existing index, per namespace ------------------
places_vs = PineconeVectorStore.from_existing_index(
    index_name="zecompete",
    embedding=embedding,
    namespace="maps"          # ← your store for place docs
)

kw_vs = PineconeVectorStore.from_existing_index(
    index_name="zecompete",
    embedding=embedding,
    namespace="keywords"      # ← your keyword-volume docs
)

# -- 4.  LLM for reasoning -------------------------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

from langchain.chains import create_retrieval_chain

def insight_question(question: str) -> str:
    """Ask a natural‑language question; get an answer grounded in
    Pinecone‑stored place vectors."""
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]