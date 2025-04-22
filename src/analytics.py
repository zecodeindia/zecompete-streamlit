# analytics.py  – vector‑powered Q&A layer
from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models  import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from src.config import secret
import pinecone

# -- 1.  init Pinecone SDK v2 -----------------------------------------
pinecone.init(
    api_key=secret("PINECONE_API_KEY"),
    environment=secret("PINECONE_ENV")
)

INDEX_NAME = "zecompete"
DIM        = 1536                      # openai text-embedding-3-small
METRIC     = "cosine"

# -- 2.  auto‑create index on first boot -------------------------------
if INDEX_NAME not in pinecone.list_indexes():
    try:
        pinecone.create_index(
            name=INDEX_NAME,
            dimension=1536,
            metric="cosine",
            pod_type="starter"   # compliant with free tier
        )
    except pinecone.core.client.exceptions.ApiException as e:
        # --- robust error logging: handles bytes, str, or None ----------
        import logging, json
        body_text = ""
        if e.body:                         # body can be None
            body_text = (
                e.body.decode()            # bytes  -> str
                if isinstance(e.body, (bytes, bytearray))
                else str(e.body)           # already str → keep
            )

        # try to parse JSON, fall back to plain text
        try:
            msg = json.loads(body_text)["message"]
        except Exception:
            msg = body_text or str(e)

        logging.error("Pinecone create_index failed: %s", msg)

# -- 3.  one embedding object (re‑used) --------------------------------
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# -- 4.  bind namespaces to that index ---------------------------------
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

# -- 5.  LLM for reasoning --------------------------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

from langchain.chains import create_retrieval_chain

def insight_question(question: str) -> str:
    """Return a narrative answer grounded in Pinecone‑stored data."""
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]
