# src/analytics.py
from pinecone import Pinecone
from src.config import secret
from langchain_pinecone import PineconeVectorStore
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate

# Initialize Pinecone
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))

# Prepare your embedding object
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# Bind to the existing index & namespaces
INDEX_NAME = "zecompete"

# Initialize the LLM
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

def insight_question(question: str) -> str:
    """
    Ask a question grounded in your Pinecone data,
    backed by LangChain Retrieval-QA.
    """
    try:
        # Determine which namespace to query based on the question
        namespace = "keywords" if any(kw in question.lower() for kw in 
                                     ["keyword", "search", "volume", "trend"]) else "maps"
        
        # Create vector store with appropriate namespace
        vector_store = PineconeVectorStore.from_existing_index(
            index_name=INDEX_NAME,
            embedding=embedding,
            namespace=namespace
        )
        
        # Create the retriever
        retriever = vector_store.as_retriever(search_kwargs={"k": 8})
        
        # Create a custom prompt for better results
        prompt = ChatPromptTemplate.from_template(
            """Answer the following question based on the provided context. 
            If the answer is not in the context, say that you don't have that information.
            
            Context: {context}
            
            Question: {input}
            
            Answer:"""
        )
        
        # Create the document chain
        document_chain = create_stuff_documents_chain(llm, prompt)
        
        # Create the retrieval chain
        chain = create_retrieval_chain(retriever, document_chain)
        
        # Execute the chain
        response = chain.invoke({"input": question})
        return response["answer"]
    
    except Exception as e:
        # Provide a meaningful error message
        return f"I encountered an error while trying to answer your question: {str(e)}. Please try a different question or check if there's data in the index."