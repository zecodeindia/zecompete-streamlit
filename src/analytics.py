from langchain.vectorstores import Pinecone as PineconeVectorStore
from langchain.chat_models import ChatOpenAI
from src.config import secret
import pinecone

pinecone.init(api_key=secret("PINECONE_API_KEY"), environment=secret("PINECONE_ENV"))
embed_dim = 1536
places_vs = PineconeVectorStore(index_name="zecompete", namespace="maps",
                                embedding_dim=embed_dim)
kw_vs     = PineconeVectorStore(index_name="zecompete", namespace="keywords",
                                embedding_dim=embed_dim)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2,
                 openai_api_key=secret("OPENAI_API_KEY"))

from langchain.chains import create_retrieval_chain

def insight_question(question: str) -> str:
    chain = create_retrieval_chain(
        retriever=places_vs.as_retriever(k=8),
        llm=llm,
        return_source_documents=False
    )
    return chain.invoke({"input": question})["answer"]
