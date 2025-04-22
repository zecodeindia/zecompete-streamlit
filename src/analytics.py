# src/analytics.py - Updated initialization
from pinecone import Pinecone
from src.config import secret
from langchain_pinecone import PineconeVectorStore  # Updated import
from langchain_openai import ChatOpenAI, OpenAIEmbeddings  # Updated import
from langchain.chains import create_retrieval_chain

# Updated Pinecone initialization
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
# No need for host parameter with just the client initialization

# Prepare your embedding object
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# Bind to the existing index & namespaces
INDEX_NAME = "zecompete"

places_vs = PineconeVectorStore.from_existing_index(
    index_name=INDEX_NAME,
    embedding=embedding,
    namespace="maps"
)

# Rest of the code remains the same...

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
